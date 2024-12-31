import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import scala.collection.mutable
import scala.collection.JavaConverters._
import java.util.Properties
import java.time.Duration

object InfluencerAnalysisConsumer {
  def main(args: Array[String]): Unit = {
    val inputTopic = "sentiment_tweets_topic"
    val outputTopic = "user_analysis_topic"

    // Kafka consumer properties
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", "localhost:9092")
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("group.id", "influencer-analysis-group")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // Kafka producer properties
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Create Kafka consumer and producer
    val consumer = new KafkaConsumer[String, String](consumerProps)
    val producer = new KafkaProducer[String, String](producerProps)

    // Subscribe to the input Kafka topic
    consumer.subscribe(java.util.Arrays.asList(inputTopic))

    println("✅ Kafka Consumer connected to topic: " + inputTopic)
    println("✅ Kafka Producer ready to send to topic: " + outputTopic)

    // Configure JSON library
    implicit val formats: DefaultFormats.type = DefaultFormats

    // Data structure to accumulate tweets by user
    val userObjects: mutable.Map[String, Map[String, Any]] = mutable.Map()

    try {
      while (true) {
        // Poll for new records
        val records = consumer.poll(Duration.ofMillis(100)).iterator()
        for (record <- records.asScala) {
          val tweetJson = record.value()

          try {
            // Parse the tweet
            val parsedTweet = parse(tweetJson).extract[Map[String, Any]]

            // Extract user and tweet information
            val userId = parsedTweet.getOrElse("user_id", "N/A").toString
            val userName = parsedTweet.getOrElse("name", "N/A").toString
            val screenName = parsedTweet.getOrElse("screen_name", "N/A").toString
            val followersCount = parsedTweet.getOrElse("followers_count", "0").toString.toInt

            val tweetId = parsedTweet.getOrElse("id", "N/A").toString
            val timestamp = parsedTweet.getOrElse("timestamp", "N/A").toString
            val geo = parsedTweet.getOrElse("geo", "N/A")
            val text = parsedTweet.getOrElse("cleaned_text", "N/A").toString
            val sentiment = parsedTweet.getOrElse("sentiment", "Neutral").toString
            val textHashed = parsedTweet.getOrElse("text_hashed", "N/A").toString
            val country = parsedTweet.getOrElse("location_info", Map()).asInstanceOf[Map[String, String]].getOrElse("country", "Unknown")

            // Prepare the tweet object
            val tweet = Map(
              "id" -> tweetId,
              "timestamp" -> timestamp,
              "geo" -> geo,
              "text" -> text,
              "text_hashed" -> textHashed,
              "sentiment" -> sentiment,
              "country" -> country
            )

            // Update or create user object
            val updatedUser = userObjects.get(userId) match {
              case Some(userObject) =>
                val existingTweets = userObject.getOrElse("tweets", List()).asInstanceOf[List[Map[String, Any]]]
                userObject + ("tweets" -> (existingTweets :+ tweet))
              case None =>
                Map(
                  "id" -> userId,
                  "name" -> userName,
                  "screen_name" -> screenName,
                  "followers_count" -> followersCount,
                  "tweets" -> List(tweet)
                )
            }

            userObjects(userId) = updatedUser

            // Determine if the user is influential
            val isInfluential = updatedUser("followers_count").asInstanceOf[Int] >= 10000 &&
              updatedUser("tweets").asInstanceOf[List[Map[String, Any]]].size >= 5

            // Prepare the user analysis object
            val userAnalysis = Map(
              "user" -> updatedUser,
              "is_influential" -> isInfluential
            )

            // Convert to JSON and send to Kafka
            val userAnalysisJson = write(userAnalysis)
            producer.send(new ProducerRecord[String, String](outputTopic, userAnalysisJson))

            // Display in console
            println("🚀 Processed User Analysis:")
            println(userAnalysisJson)
            println("------------------------------------------------")
          } catch {
            case e: Exception =>
              println(s"❌ Error processing tweet: ${e.getMessage}")
          }
        }
      }
    } finally {
      // Close consumer and producer
      consumer.close()
      producer.close()
      println("🚪 Kafka Consumer and Producer closed.")
    }
  }
}