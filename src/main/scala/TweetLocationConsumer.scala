import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.SimpleFeatureSource
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}
import org.geotools.geometry.jts.JTSFactoryFinder
import scala.collection.JavaConverters._
import java.util.Properties
import java.time.Duration
import java.io.File

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

    // Path to the Shapefile
    val shapefilePath = "C:/Users/HP/Downloads/ne_10m_admin_0_countries/ne_10m_admin_0_countries.shp"
    val shapefile = new ShapefileDataStore(new File(shapefilePath).toURI.toURL)
    val featureSource: SimpleFeatureSource = shapefile.getFeatureSource

    // GeoTools setup
    val geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

    implicit val formats: DefaultFormats.type = DefaultFormats

    try {
      while (true) {
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
        for (record <- records.asScala) {
          val cleanedTweet = record.value()

          println("🚀 Received cleaned tweet:")
          println(cleanedTweet)

          try {
            val parsedTweet = parse(cleanedTweet).extract[Map[String, Any]]
            println("📜 Parsed cleaned tweet.")

            // Extract 'geo' field
            val geo = parsedTweet.get("geo") match {
              case Some(map: Map[String, Any]) =>
                map.get("coordinates") match {
                  case Some(coords: List[Double]) => coords
                  case _ => Nil
                }
              case _ => Nil
            }

            val country = if (geo.nonEmpty) {
              val latitude = geo.head
              val longitude = geo(1)
              val point: Point = geometryFactory.createPoint(new Coordinate(longitude, latitude))

              // Match point with country from Shapefile
              val features = featureSource.getFeatures
              val iterator = features.features()
              var foundCountry = "Unknown Country"

              try {
                while (iterator.hasNext) {
                  val feature = iterator.next()
                  val geometry = feature.getDefaultGeometry.asInstanceOf[org.locationtech.jts.geom.Geometry]

                  if (geometry.contains(point)) {
                    foundCountry = feature.getAttribute("NAME").toString
                    println(s"🌍 Tweet Location: $foundCountry")
                    iterator.close()
                    foundCountry
                  }
                }
              } finally {
                iterator.close()
              }
              foundCountry
            } else {
              "Geo data not available"
            }

            // Avoid repeating the location in the tweet
            println(s"🌍 Location Info: Country -> $country")
            println("------------------------------------------------")

            // Prepare new data for Kafka by updating tweet without repeating the location
            val updatedTweet = parsedTweet + ("location_info" -> Map("country" -> country))
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
