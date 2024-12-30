import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._
import java.util.Properties
import java.time.Duration
import java.security.MessageDigest

object TweetFeatureExtractionConsumer {
  def main(args: Array[String]): Unit = {
    val inputTopic = "cleaned_tweets_topic" // Kafka topic containing cleaned tweets
    val outputTopic = "hashed_tweets_topic" // Kafka topic for hashed tweets

    // Kafka consumer properties
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", "localhost:9092")
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("group.id", "hashing-consumer-group")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // Kafka producer properties
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Create Kafka consumer and producer
    val consumer = new KafkaConsumer[String, String](consumerProps)
    val producer = new KafkaProducer[String, String](producerProps)
    consumer.subscribe(java.util.Arrays.asList(inputTopic))

    println("✅ Kafka Consumer connected to localhost:9092")
    println("✅ Kafka Producer ready to send to topic: " + outputTopic)

    implicit val formats: DefaultFormats.type = DefaultFormats

    try {
      while (true) {
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
        for (record <- records.asScala) {
          val cleanedTweet = record.value()

          try {
            val parsedTweet = parse(cleanedTweet).extract[Map[String, Any]]
            println("📜 Parsed cleaned tweet.")

            // Extract the required fields
            val user = parsedTweet.get("user") match {
              case Some(userMap: Map[String, Any]) =>
                val userId = userMap.getOrElse("id", "N/A")
                val screenName = userMap.getOrElse("screen_name", "N/A")
                val name = userMap.getOrElse("name", "N/A")
                val followersCount = userMap.getOrElse("followers_count", "N/A")
                (userId, screenName, name, followersCount)
              case _ => ("N/A", "N/A", "N/A", "N/A")
            }

            val tweetDetails = parsedTweet match {
              case tweetMap: Map[String, Any] =>
                val baseTweet = tweetMap.get("tweet_details") match {
                  case Some(retweetedStatus: Map[String, Any]) => retweetedStatus
                  case None => tweetMap
                }

                val tweetId = baseTweet.getOrElse("id", "N/A").toString
                val timestamp = baseTweet.getOrElse("created_at", "N/A").toString
                val geo = baseTweet.getOrElse("geo", "N/A").toString
                val text = baseTweet.getOrElse("text", "N/A").toString
                val retweetCount = baseTweet.getOrElse("retweet_count", 0).toString.toInt
                val favoriteCount = baseTweet.getOrElse("favorite_count", 0).toString.toInt

                (tweetId, timestamp, geo, text, retweetCount, favoriteCount)
              case _ =>
                // In case of parsing failure
                ("N/A", "N/A", "N/A", "N/A", 0, 0)
            }

            // Clean and hash the text
            val cleanedText = cleanText(tweetDetails._4.toString)
            val hashedText = computeHash(cleanedText)

            // Print the extracted fields and cleaned text
            println(s"User ID: ${user._1}")
            println(s"Screen Name: ${user._2}")
            println(s"Name: ${user._3}")
            println(s"Followers Count: ${user._4}")
            println(s"Tweet ID: ${tweetDetails._1}")
            println(s"Timestamp: ${tweetDetails._2}")
            println(s"Geo: ${tweetDetails._3}")
            println(s"Retweet Count: ${tweetDetails._5}")
            println(s"Favorite Count: ${tweetDetails._6}")
            println(s"Cleaned Text: $cleanedText")
            println(s"Text (hashed): $hashedText")
            println("---------")

            // Prepare the updated tweet to store exactly what is displayed
            val updatedTweet = Map(
              "user_id" -> user._1,
              "screen_name" -> user._2,
              "name" -> user._3,
              "followers_count" -> user._4,
              "tweet_id" -> tweetDetails._1,
              "timestamp" -> tweetDetails._2,
              "geo" -> tweetDetails._3,
              "retweet_count" -> tweetDetails._5,
              "favorite_count" -> tweetDetails._6,
              "cleaned_text" -> cleanedText,
              "text_hashed" -> hashedText
            )

            val updatedTweetJson = Serialization.write(updatedTweet)

            // Send to Kafka output topic
            producer.send(new ProducerRecord[String, String](outputTopic, updatedTweetJson))
            println("📤 Updated tweet sent to Kafka topic: " + outputTopic)
          } catch {
            case e: Exception =>
              println(s"❌ Error processing tweet: ${e.getMessage}")
          }
        }
      }
    } finally {
      consumer.close()
      producer.close()
      println("🚪 Kafka Consumer and Producer closed.")
    }
  }

  def cleanText(text: String): String = {
    text
      .replaceAll("""@""", "") // Remove '@' but keep the word
      .replaceAll("""#""", "") // Remove '#' but keep the word
      .replaceAll("\\s{2,}", " ") // Replace multiple spaces with a single space
      .trim // Remove leading and trailing spaces
  }

  def computeHash(data: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val hashBytes = md.digest(data.getBytes("UTF-8"))
    hashBytes.map("%02x".format(_)).mkString
  }
}
