import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write

import scala.collection.JavaConverters._
import java.util.Properties
import java.time.Duration

object CleaningConsumerApp {
  def main(args: Array[String]): Unit = {
    val inputTopic = "tweets_topic"
    val outputTopic = "cleaned_tweets_topic"

    // Kafka consumer properties
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", "localhost:9092")
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("group.id", "tweet-consumer-group")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // Kafka producer properties
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Create Kafka Consumer and Producer
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

          // Parse and display the tweet in the required format
          val parsedTweet = parseTweet(rawTweet)

          if (parsedTweet.nonEmpty) {
            displayTweet(parsedTweet)

            // Convert parsed tweet to JSON and send to Kafka
            val cleanedTweetJson = write(parsedTweet)
            producer.send(new ProducerRecord[String, String](outputTopic, cleanedTweetJson))
            println(s"📤 Cleaned tweet sent to Kafka topic: $outputTopic")
          } else {
            println("❌ Tweet skipped due to invalid format.")
          }
        }
      }
    } catch {
      case e: Exception =>
        println(s"❌ Error: ${e.getMessage}")
    } finally {
      consumer.close()
      producer.close()
      println("🚪 Kafka Consumer and Producer closed.")
    }
  }

  // Function to parse the tweet into the required format
  def parseTweet(tweet: String): Map[String, Any] = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    try {
      // Parse the tweet into a JSON object
      val tweetJson = parse(tweet).extract[Map[String, Any]]

      // Extract user information
      val userField = tweetJson.get("user") match {
        case Some(user: Map[String, Any]) =>
          Map(
            "id" -> user.getOrElse("id", "N/A"),
            "name" -> user.getOrElse("name", "N/A"),
            "screen_name" -> user.getOrElse("screen_name", "N/A"),
            "followers_count" -> user.getOrElse("followers_count", 0)
          )
        case _ => Map.empty
      }

      // Extract tweet details
      val tweetDetails = Map(
        "id" -> tweetJson.getOrElse("id", "N/A"),
        "created_at" -> tweetJson.getOrElse("created_at", "N/A"),
        "geo" -> tweetJson.getOrElse("geo", "N/A"),
        "text" -> tweetJson.getOrElse("text", "N/A"),
        "retweet_count" -> tweetJson.getOrElse("retweetCount", 0),
        "favorite_count" -> tweetJson.getOrElse("favoriteCount", 0)
      )

      // Combine user and tweet information
      Map("user" -> userField, "tweet_details" -> tweetDetails)
    } catch {
      case _: Exception =>
        println(s"❌ Failed to parse tweet: $tweet")
        Map.empty
    }
  }

  // Function to display the tweet in the required format
  def displayTweet(tweet: Map[String, Any]): Unit = {
    tweet.get("user") match {
      case Some(user: Map[String, Any]) =>
        println(s"User ID: ${user.getOrElse("id", "N/A")}")
        println(s"Name: ${user.getOrElse("name", "N/A")}")
        println(s"Screen Name: ${user.getOrElse("screen_name", "N/A")}")
        println(s"Followers Count: ${user.getOrElse("followers_count", 0)}")

        val tweetDetails = tweet.getOrElse("tweet_details", Map.empty).asInstanceOf[Map[String, Any]]
        println(s"Tweet ID: ${tweetDetails.getOrElse("id", "N/A")}")
        println(s"Timestamp: ${tweetDetails.getOrElse("created_at", "N/A")}")
        println(s"Geo: ${tweetDetails.getOrElse("geo", "N/A")}")
        println(s"Text: ${tweetDetails.getOrElse("text", "N/A")}")
        println(s"Retweet Count: ${tweetDetails.getOrElse("retweet_count", 0)}")
        println(s"Favorite Count: ${tweetDetails.getOrElse("favorite_count", 0)}")

      case _ => println("User: N/A")
    }

    println("------------------------------")
  }
}
