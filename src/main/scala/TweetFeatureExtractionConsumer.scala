import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import java.security.MessageDigest
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._
import java.util.Properties
import java.time.Duration

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

          // Debugging received data
          println("🚀 Received cleaned tweet:")
          println(cleanedTweet)

          try {
            val parsedTweet = parse(cleanedTweet).extract[Map[String, Any]]
            println("📜 Parsed cleaned tweet.")

            val text = parsedTweet.get("text") match {
              case Some(t: String) => cleanText(t)
              case _ => "N/A"
            }

            val geo = parsedTweet.get("geo") match {
              case Some(g: String) => g
              case _ => "N/A"
            }

            val user = parsedTweet.get("user") match {
              case Some(userMap: Map[String, Any]) =>
                userMap.get("name") match {
                  case Some(name: String) => name
                  case _ => "N/A"
                }
              case _ => "N/A"
            }

            val concatenatedFields = s"$text|$geo|$user"
            val hashValue = computeHash(concatenatedFields)

            // Print the updates
            println(s"🔧 Update:")
            println(s"  Text: $text")
            println(s"  Geo: $geo")
            println(s"  User: $user")
            println(s"🔒 Hash: $hashValue")
            println("------------------------------------------------")

            // Prepare new data for Kafka
            val updatedTweet = Map(
              "text" -> text,
              "geo" -> geo,
              "user" -> user,
              "hash" -> hashValue
            )
            val updatedTweetJson = org.json4s.jackson.Serialization.write(updatedTweet)

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
    // Remove only the '@' and '#' characters but keep the words
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

