import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import scala.io.StdIn

object KafkaProducerApp {
  def main(args: Array[String]): Unit = {
    // Kafka topic
    val topic = "tweets_topic"

    // Kafka producer properties
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092") // Kafka broker in Docker
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Create the Kafka producer
    val producer = new KafkaProducer[String, String](props)

    println(s"Kafka Producer started. Type messages to send to topic '$topic' (type 'exit' to quit):")

    try {
      var input = ""
      while ({ input = StdIn.readLine(); input != "exit" }) {
        val record = new ProducerRecord[String, String](topic, "key", input)
        producer.send(record)
        println(s"Sent: $input")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
      println("Kafka Producer closed.")
    }
  }
}
