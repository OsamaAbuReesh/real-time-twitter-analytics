import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import scala.collection.JavaConverters._
import java.util.Properties
import java.time.Duration

object TweetLocationConsumer {
  def main(args: Array[String]): Unit = {
    val inputTopic = "cleaned_tweets_topic" // Kafka topic containing cleaned tweets
    val outputTopic = "location_tweets_topic" // Kafka topic for tweets with location details

    // Kafka consumer properties
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", "localhost:9092")
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("group.id", "location-consumer-group")
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

            // Extract 'geo' field
            val geo = parsedTweet.get("geo") match {
              case Some(map: Map[String, Any]) =>
                map.get("coordinates") match {
                  case Some(coords: List[Double]) =>
                    val latitude = coords.headOption.getOrElse(0.0)
                    val longitude = coords.lift(1).getOrElse(0.0)
                    s"Lat: $latitude, Lon: $longitude"
                  case _ => "Coordinates not available"
                }
              case _ => "Geo data not available"
            }

            // Determine country based on simple rules (example only)
            val country = geo match {
              case g if g.contains("USA") => "United States"
              case g if g.contains("UK") => "United Kingdom"
              case g if g.contains("Canada") => "Canada"
              case _ => "Unknown Country"
            }

            // Print location information
            println(s"🌍 Location Info:")
            println(s"  Geo: $geo")
            println(s"  Country: $country")
            println("------------------------------------------------")

            // Prepare new data for Kafka
            val updatedTweet = parsedTweet + ("geo_info" -> geo, "country" -> country)
            val updatedTweetJson = org.json4s.jackson.Serialization.write(updatedTweet)

            // Send to Kafka output topic
            producer.send(new ProducerRecord[String, String](outputTopic, updatedTweetJson))
            println("📤 Updated tweet with location info sent to Kafka topic: " + outputTopic)
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
}