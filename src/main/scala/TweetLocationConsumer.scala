import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization
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
    val inputTopic = "hashed_tweets_topic" // Kafka topic containing hashed tweets
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

    val consumer = new KafkaConsumer[String, String](consumerProps)
    val producer = new KafkaProducer[String, String](producerProps)
    consumer.subscribe(java.util.Arrays.asList(inputTopic))

    println("✅ Kafka Consumer connected to topic: " + inputTopic)
    println("✅ Kafka Producer ready to send to topic: " + outputTopic)

    // Path to the Shapefile
    val shapefilePath = "C:\\real-time-twitter-analytics6\\ne_10m_admin_0_countries\\ne_10m_admin_0_countries.shp"
    val shapefile = new ShapefileDataStore(new File(shapefilePath).toURI.toURL)
    val featureSource: SimpleFeatureSource = shapefile.getFeatureSource

    // GeoTools setup
    val geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

    implicit val formats: DefaultFormats.type = DefaultFormats

    try {
      while (true) {
        val records: ConsumerRecords[String, String] = consumer.poll(Duration.ofMillis(100))
        for (record <- records.asScala) {
          val hashedTweet = record.value()

          try {
            val parsedTweet = parse(hashedTweet).extract[Map[String, Any]]

            // Extract fields
            val userId = parsedTweet.getOrElse("user_id", "Unknown").toString
            val screenName = parsedTweet.getOrElse("screen_name", "Unknown").toString
            val name = parsedTweet.getOrElse("name", "Unknown").toString
            val followersCount = parsedTweet.getOrElse("followers_count", 0).toString
            val tweetId = parsedTweet.getOrElse("id", "Unknown").toString
            val timestamp = parsedTweet.getOrElse("timestamp", "Unknown").toString
            val geo = parsedTweet.get("geo") match {
              case Some(map: Map[String, Any]) =>
                map.get("coordinates") match {
                  case Some(coords: List[Double]) => coords
                  case _ => Nil
                }
              case _ => Nil
            }
            val retweetCount = parsedTweet.getOrElse("retweet_count", 0).toString
            val favoriteCount = parsedTweet.getOrElse("favorite_count", 0).toString
            val cleanedText = parsedTweet.getOrElse("cleaned_text", "Unknown").toString

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
                    iterator.close()
                  }
                }
              } finally {
                iterator.close()
              }
              foundCountry
            } else {
              "Geo data not available"
            }

            // Prepare new data for Kafka by updating tweet
            val updatedTweet = parsedTweet + ("location_info" -> Map("country" -> country))
            val updatedTweetJson = Serialization.writePretty(updatedTweet) // Use pretty JSON formatting

            // Print formatted output
            println(
              s"""
                 |User ID: $userId
                 |Screen Name: $screenName
                 |Name: $name
                 |Followers Count: $followersCount
                 |Tweet ID: $tweetId
                 |Timestamp: $timestamp
                 |Geo: ${geo.mkString(", ")}
                 |Retweet Count: $retweetCount
                 |Favorite Count: $favoriteCount
                 |Cleaned Text: $cleanedText
                 |Country: $country
                 |------------------------------------------------
               """.stripMargin)

            // Send to Kafka output topic
            producer.send(new ProducerRecord[String, String](outputTopic, updatedTweetJson))
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
