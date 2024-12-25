import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.json4s._
import org.json4s.jackson.JsonMethods._

import scala.collection.JavaConverters._
import java.util.Properties
import java.time.Duration

object CleaningConsumerApp {
  def main(args: Array[String]): Unit = {
    val topic = "tweets_topic"

    // Kafka consumer properties
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "tweet-consumer-group")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // Create the Kafka consumer
    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(java.util.Arrays.asList("tweets_topic"))

    println("✅ Kafka Consumer connected to localhost:9092")

    implicit val formats: DefaultFormats.type = DefaultFormats

    try {
      while (true) {
        // Poll the Kafka topic for new records
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
        for (record <- records.asScala) {
          val rawTweet = record.value()

          // Clean the tweet and display only the required fields
          val cleanedTweet = cleanTweet(rawTweet)

          println("✨ Cleaned tweet (formatted):")
          displayCleanedTweet(cleanedTweet)
        }
      }
    } catch {
      case e: Exception =>
        println(s"❌ Error: ${e.getMessage}")
    } finally {
      consumer.close()
      println("🚪 Kafka Consumer closed.")
    }
  }

  // Function to clean the tweet
  def cleanTweet(tweet: String): Map[String, Any] = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    try {
      // Parse the tweet into a JSON object
      val tweetJson = parse(tweet).extract[Map[String, Any]]

      // Extract and format the user field
      val userField = tweetJson.get("user") match {
        case Some(user: Map[String, Any]) =>
          Map(
            "id" -> user.getOrElse("id", "N/A"),
            "name" -> user.getOrElse("name", "N/A")
          )
        case _ => Map.empty
      }

      // Extract the text of the tweet
      val textField = tweetJson.getOrElse("text", "N/A")

      // Extract only the required fields
      Map(
        "geo" -> tweetJson.getOrElse("geo", "N/A"),
        "user" -> userField,
        "id" -> tweetJson.getOrElse("id", "N/A"),
        "id_str" -> tweetJson.getOrElse("id_str", "N/A"),
        "timestamp" -> tweetJson.getOrElse("created_at", "N/A"),
        "text" -> textField // Add text field here
      )
    } catch {
      case _: Exception =>
        println(s"❌ Failed to clean tweet: $tweet")
        Map.empty // Return an empty map in case of failure
    }
  }

  // Function to display the cleaned tweet in a formatted way
  def displayCleanedTweet(cleanedTweet: Map[String, Any]): Unit = {
    println(s"Geo: ${cleanedTweet.getOrElse("geo", "N/A")}")
    cleanedTweet.get("user") match {
      case Some(user: Map[String, Any]) =>
        println(s"User: ID -> ${user.getOrElse("id", "N/A")}, Name -> ${user.getOrElse("name", "N/A")}")
      case _ => println("User: N/A")
    }
    println(s"ID: ${cleanedTweet.getOrElse("id", "N/A")}")
    println(s"ID String: ${cleanedTweet.getOrElse("id_str", "N/A")}")
    println(s"Timestamp: ${cleanedTweet.getOrElse("timestamp", "N/A")}")
    println(s"Text: ${cleanedTweet.getOrElse("text", "N/A")}") // Print the tweet text
    println("------------------------------------------------")
  }
}
