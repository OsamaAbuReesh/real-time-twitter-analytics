import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.util.Properties
import scala.io.Source

object KafkaProducerApp {
  def main(args: Array[String]): Unit = {
    // Kafka topic name
    val topic = "tweets_topic"

    // Kafka producer properties
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092") // Change to "kafka:9092" if running inside Docker
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Create the Kafka producer
    val producer = new KafkaProducer[String, String](props)
    println(s"✅ Kafka Producer connected to localhost:9092")

    // Path to the JSON file containing tweet data
    val filePath = "boulder_flood_geolocated_tweets.json"

    try {
      // Read the file line-by-line (handle NDJSON)
      println(s"📄 Reading data from $filePath")
      val source = Source.fromFile(filePath)
      val tweets = source.getLines().toList // Each line is a separate JSON tweet
      source.close()

      println(s"📦 Successfully read ${tweets.length} tweets from $filePath")

      // JSON formats required for json4s
      implicit val formats: DefaultFormats.type = DefaultFormats

      // Send tweets to Kafka in batches
      val batchSize = 1000  // Number of tweets to send at once
      val delay = 1000   // Delay between each batch in milliseconds

      tweets.grouped(batchSize).zipWithIndex.foreach { case (batch, batchNumber) =>
        batch.zipWithIndex.foreach { case (tweetLine, index) =>
          try {
            // Parse the tweet line into a JSON object
            val tweet = parse(tweetLine).extract[Map[String, Any]]

            // Convert tweet to JSON string
            val tweetJson = compact(render(Extraction.decompose(tweet)))

            // Send the tweet to Kafka
            val record = new ProducerRecord[String, String](topic, index.toString, tweetJson)
            producer.send(record)

            // Log successful tweet send
            println(s"📤 Sent tweet [batch $batchNumber, index $index] with id: ${tweet.getOrElse("id", "N/A")}")
          } catch {
            case e: Exception =>
              // Log message for JSON parse failure
              println(s"❌ Failed to parse and send tweet [batch $batchNumber, index $index]. Error: ${e.getMessage}")
          }
        }

        // Wait for a few seconds before sending the next batch
        println(s"⏳ Waiting for $delay ms before sending the next batch...")
        Thread.sleep(delay)
      }

    } catch {
      case e: Exception =>
        println(s"❌ Failed to read file $filePath. Error: ${e.getMessage}")
    } finally {
      // Close the producer after all tweets are sent
      producer.close()
      println("🚪 Kafka Producer closed.")
    }
  }
}