import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import scala.collection.JavaConverters._
import java.util.Properties
import java.time.Duration

// Application for cleaning and filtering tweets
object CleaningConsumerApp {
  def main(args: Array[String]): Unit = {
    val inputTopic = "tweets_topic"
    val outputTopic = "cleaned_tweets_topic"

    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", "localhost:9092")
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("group.id", "tweet-consumer-group")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val consumer = new KafkaConsumer[String, String](consumerProps)
    val producer = new KafkaProducer[String, String](producerProps)
    consumer.subscribe(java.util.Arrays.asList(inputTopic))

    println("✅ Kafka Consumer connected to localhost:9092")

    implicit val formats: DefaultFormats.type = DefaultFormats

    try {
      while (true) {
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
        for (record <- records.asScala) {
          val rawTweet = record.value()
          val cleanedTweet = parseAndCleanTweet(rawTweet)

          if (cleanedTweet.nonEmpty) {
            displayTweet(cleanedTweet)
            val cleanedTweetJson = write(cleanedTweet)
            producer.send(new ProducerRecord[String, String](outputTopic, cleanedTweetJson))
            println(s"📤 Cleaned tweet sent to Kafka topic: $outputTopic")
          } else {
            println("❌ Tweet skipped due to invalid format.")
          }
        }
      }
    } finally {
      consumer.close()
      producer.close()
      println("🚪 Kafka Consumer and Producer closed.")
    }
  }

  def parseAndCleanTweet(tweet: String): Map[String, AnyRef] = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    try {
      val tweetJson = parse(tweet).extract[Map[String, AnyRef]]

      // Extract user fields
      val userField = tweetJson.get("user") match {
        case Some(user: Map[String, AnyRef]) =>
          Map(
            "id" -> user.getOrElse("id", "N/A"),
            "name" -> user.getOrElse("name", "N/A"),
            "screen_name" -> user.getOrElse("screen_name", "N/A"),
            "followers_count" -> user.getOrElse("followers_count", 0)
          )
        case _ => Map.empty
      }

      // Extract and clean tweet details
      val rawText = tweetJson.getOrElse("text", "N/A").toString
      val cleanedText = cleanText(rawText)

      val tweetDetails = Map(
        "id" -> tweetJson.getOrElse("id", "N/A"),
        "created_at" -> tweetJson.getOrElse("created_at", "N/A"),
        "geo" -> tweetJson.getOrElse("geo", "N/A"),
        "text" -> cleanedText,
        "retweet_count" -> tweetJson.getOrElse("retweet_count", 0),
        "favorite_count" -> tweetJson.getOrElse("favorite_count", 0)
      )

      Map("user" -> userField, "tweet_details" -> tweetDetails)
    } catch {
      case _: Exception =>
        println(s"❌ Failed to parse tweet: $tweet")
        Map.empty
    }
  }

  def cleanText(text: String): String = {
    // Remove URLs, hashtags, and mentions
    text.replaceAll("https?://\\S+\\s?", "") // Remove URLs
      .replaceAll("#\\S+", "") // Remove hashtags
      .replaceAll("@\\S+", "") // Remove mentions
      .trim // Remove extra spaces
  }

  def displayTweet(tweet: Map[String, AnyRef]): Unit = {
    // Display only the selected fields
    tweet.get("user") match {
      case Some(user: Map[String, AnyRef]) =>
        println(s"User ID: ${user.getOrElse("id", "N/A")}")
        println(s"Name: ${user.getOrElse("name", "N/A")}")
        println(s"Screen Name: ${user.getOrElse("screen_name", "N/A")}")
        println(s"Followers Count: ${user.getOrElse("followers_count", 0)}")
      case _ => println("User: N/A")
    }

    tweet.get("tweet_details") match {
      case Some(details: Map[String, AnyRef]) =>
        println(s"Tweet ID: ${details.getOrElse("id", "N/A")}")
        println(s"Timestamp: ${details.getOrElse("created_at", "N/A")}")
        println(s"Geo: ${details.getOrElse("geo", "N/A")}")
        println(s"Text: ${details.getOrElse("text", "N/A")}")
        println(s"Retweet Count: ${details.getOrElse("retweet_count", 0)}")
        println(s"Favorite Count: ${details.getOrElse("favorite_count", 0)}")
      case _ => println("Tweet Details: N/A")
    }

    println("------------------------------")
  }
}
