
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.client.{Request, RestClient, RestClientBuilder}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import java.util.Properties
import scala.collection.JavaConverters._
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder

object KafkaToElasticsearchConsumer {
  def main(args: Array[String]): Unit = {

    // Load configurations from environment variables
    val kafkaBootstrapServers = sys.env.getOrElse("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    val kafkaGroupId = sys.env.getOrElse("KAFKA_GROUP_ID", "scala-consumer-group")
    val kafkaTopic = sys.env.getOrElse("KAFKA_TOPIC", "user_analysis_topic")
    val esHost = sys.env.getOrElse("ELASTICSEARCH_HOST", "localhost")
    val esPort = sys.env.getOrElse("ELASTICSEARCH_PORT", "9200").toInt
    val esUsername = sys.env.getOrElse("ELASTICSEARCH_USERNAME", "elastic")
    val esPassword = sys.env.getOrElse("ELASTICSEARCH_PASSWORD", "your_password_here")

    // Configure Kafka consumer
    val consumerProps = new Properties()
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId)
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    consumerProps.put("key.deserializer", classOf[StringDeserializer].getName)
    consumerProps.put("value.deserializer", classOf[StringDeserializer].getName)

    val consumer = new KafkaConsumer[String, String](consumerProps)
    consumer.subscribe(java.util.Arrays.asList(kafkaTopic))

    println(s"✅ Kafka Consumer connected to topic: $kafkaTopic")

    // Configure Elasticsearch client with credentials
    val credentialsProvider = new BasicCredentialsProvider()
    credentialsProvider.setCredentials(
      AuthScope.ANY,
      new UsernamePasswordCredentials(esUsername, esPassword)
    )

    val client: RestClient = RestClient.builder(new HttpHost(esHost, esPort, "http"))
      .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
        override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
          httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        }
      }).build()

    // Index name in Elasticsearch
    val indexName = "user_analysis1"

    // Consume messages from Kafka and write to Elasticsearch
    try {
      while (true) {
        val records = consumer.poll(java.time.Duration.ofMillis(1000)).iterator()
        while (records.hasNext) {
          val record = records.next()
          val message = record.value()
          println(s"Received message: $message")

          try {
            // Parse message to JSON
            implicit val formats: DefaultFormats.type = DefaultFormats
            val parsedData = parse(message).extract[Map[String, Any]]

            // Convert data to JSON string
            val json = write(parsedData)

            // Create and execute request to Elasticsearch
            val request = new Request("POST", s"/$indexName/_doc/")
            request.setJsonEntity(json)
            client.performRequest(request)

            println("Data written to Elasticsearch.")
          } catch {
            case e: Exception =>
              println(s"Error parsing message: ${e.getMessage}")
          }
        }
      }
    } finally {
      // Close consumer and client after completion
      consumer.close()
      client.close()
      println("🚪 Kafka Consumer and Elasticsearch Client closed.")
    }
  }
}
