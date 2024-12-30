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

          try {
            val parsedTweet = parse(cleanedTweet).extract[Map[String, Any]]
            println("📜 Parsed cleaned tweet.")

            // Extract user details
            val user = parsedTweet.get("user") match {
              case Some(userMap: Map[String, Any]) =>
                val userId = userMap.getOrElse("id", "N/A")
                val screenName = userMap.getOrElse("screen_name", "N/A")
                val name = userMap.getOrElse("name", "N/A")
                val followersCount = userMap.getOrElse("followers_count", "N/A")
                (userId, screenName, name, followersCount)
              case _ => ("N/A", "N/A", "N/A", "N/A")
            }

            // Extract tweet details
            val tweetDetails = parsedTweet.get("tweet_details") match {
              case Some(tweet: Map[String, Any]) =>
                val tweetId = tweet.getOrElse("id", "N/A")
                val timestamp = tweet.getOrElse("created_at", "N/A")
                val geo = tweet.getOrElse("geo", "N/A")
                val text = tweet.getOrElse("text", "N/A")
                val retweetCount = tweet.getOrElse("retweet_count", 0)
                val favoriteCount = tweet.getOrElse("favorite_count", 0)
                (tweetId, timestamp, geo, text, retweetCount, favoriteCount)
              case _ => ("N/A", "N/A", "N/A", "N/A", 0, 0)
            }

            // Clean and hash the text
            val cleanedText = cleanText(tweetDetails._4.toString)
            val hashedText = computeHash(cleanedText)

            // Determine the location using geo-coordinates
            val geo = tweetDetails._3
            val country = geo match {
              case coordinates: Map[String, Any] =>
                val lat = coordinates.getOrElse("coordinates", Nil) match {
                  case List(lat: Double, _) => lat
                  case _ => 0.0
                }
                val long = coordinates.getOrElse("coordinates", Nil) match {
                  case List(_, long: Double) => long
                  case _ => 0.0
                }

                val point: Point = geometryFactory.createPoint(new Coordinate(long, lat))

                // Match point with country from Shapefile
                val features = featureSource.getFeatures
                val iterator = features.features()
                var foundCountry = "Unknown Country"

                try {
                  while (iterator.hasNext) {
                    val feature = iterator.next()
                    val geometry = feature.getDefaultGeometry.asInstanceOf[org.locationtech.jts.geom.Geometry]

                    if (geometry.contains(point)) {
                      foundCountry = feature.getAttribute("NAME").toString  // Assuming "NAME" is the country name field
                      println(s"🌍 Tweet Location: $foundCountry")  // Print tweet location for each tweet
                    }
                  }
                } finally {
                  iterator.close()
                }

                foundCountry
              case _ => "Geo data not available"
            }

            // Print the extracted fields and cleaned text
            println(s"User ID: ${user._1}")
            println(s"Screen Name: ${user._2}")
            println(s"Name: ${user._3}")
            println(s"Followers Count: ${user._4}")
            println(s"Tweet ID: ${tweetDetails._1}")
            println(s"Timestamp: ${tweetDetails._2}")
            println(s"Geo: ${tweetDetails._3}")
            println(s"Retweet_Count: ${tweetDetails._5}")
            println(s"Favorite_Count: ${tweetDetails._6}")
            println(s"Cleaned Text: $cleanedText")
            println(s"Text (hashed): $hashedText")
            println(s"Location Info: Country -> $country")
            println("------------------------------------------------")

            // Prepare updated tweet JSON
            val updatedTweet = parsedTweet ++ Map(
              "cleaned_text" -> cleanedText,
              "hashed_text" -> hashedText,
              "location_info" -> Map("country" -> country)
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

  // Helper functions for cleaning and hashing text
  def cleanText(text: String): String = {
    text.replaceAll("[^a-zA-Z0-9\\s]", "").toLowerCase.trim
  }

  def computeHash(text: String): String = {
    java.security.MessageDigest.getInstance("SHA-256")
      .digest(text.getBytes("UTF-8"))
      .map("%02x".format(_)).mkString
  }
}
