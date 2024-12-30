import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.util.Properties
import java.time.Duration
import scala.jdk.CollectionConverters._
import org.geotools.data.shapefile.ShapefileDataStore
import org.geotools.data.simple.SimpleFeatureSource
import org.locationtech.jts.geom.{Coordinate, GeometryFactory, Point}
import org.geotools.geometry.jts.JTSFactoryFinder
import java.io.File

object SentimentAnalysisConsumer {
  def main(args: Array[String]): Unit = {
    val inputTopic = "cleaned_tweets_topic"
    val outputTopic = "sentiment_tweets_topic"

    // Kafka consumer properties
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", "localhost:9092")
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("group.id", "sentiment-consumer-group")
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    // Kafka producer properties
    val producerProps = new Properties()
    producerProps.put("bootstrap.servers", "localhost:9092")
    producerProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    producerProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Create Kafka consumer and producer
    val consumer = new KafkaConsumer[String, String](consumerProps)
    val producer = new KafkaProducer[String, String](producerProps)

    // Subscribe to the input Kafka topic
    consumer.subscribe(java.util.Arrays.asList(inputTopic))

    println("✅ Kafka Consumer connected to localhost:9092")
    println("✅ Kafka Producer ready to send to topic: " + outputTopic)

    // Configure JSON library
    implicit val formats: DefaultFormats.type = DefaultFormats

    // Define sentiment words (in English)
    val happyWords = Set(
      "happy", "joyful", "cheerful", "excited", "delighted", "elated", "glad", "fantastic",
      "amazing", "thrilled", "overjoyed", "blissful", "content", "joyous", "ecstatic", "pleased",
      "radiant", "great", "wonderful", "positive", "smiling"
    )

    val sadWords = Set(
      "sad", "depressed", "down", "gloomy", "heartbroken", "mournful", "disappointed", "melancholy",
      "painful", "hopeless", "low", "unhappy", "blue", "grief", "despair", "isolated", "sorrowful",
      "gloom", "dismal", "regretful", "mournful", "sickened", "woeful"
    )

    val neutralWords = Set(
      "neutral", "average", "normal", "okay", "fine", "stable", "neutral mood", "nothing special",
      "calm", "moderate", "ordinary", "nothing new", "indifferent", "balanced", "level-headed", "standard"
    )

    // Path to the Shapefile
    val shapefilePath = "C:/Users/HP/Downloads/ne_10m_admin_0_countries/ne_10m_admin_0_countries.shp"
    val shapefile = new ShapefileDataStore(new File(shapefilePath).toURI.toURL)
    val featureSource: SimpleFeatureSource = shapefile.getFeatureSource

    // GeoTools setup
    val geometryFactory: GeometryFactory = JTSFactoryFinder.getGeometryFactory

    try {
      while (true) {
        // Poll for new records
        val records = consumer.poll(Duration.ofMillis(100)).iterator()
        for (record <- records.asScala) {
          val cleanedTweet = record.value()
          println("🚀 Received cleaned tweet:")
          //println(cleanedTweet)

          try {
            // Parse the tweet
            val parsedTweet = parse(cleanedTweet).extract[Map[String, Any]]
            val text = parsedTweet.get("text") match {
              case Some(t: String) => t
              case _ => "N/A"
            }

            // Analyze sentiment
            val sentiment = analyzeSentiment(text, happyWords, sadWords, neutralWords)
            println(s"🔧 Sentiment: $sentiment")

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
            println(s"Tweet ID: ${tweetDetails._1}")
            println(s"Timestamp: ${tweetDetails._2}")
            println(s"Geo: ${tweetDetails._3}")
            println(s"Text: ${tweetDetails._4}")
            println(s"Retweet_Count: ${tweetDetails._5}")
            println(s"Favorite_Count: ${tweetDetails._6}")
            println(s"Cleaned Text: $cleanedText")
            println(s"Text (hashed): $hashedText")
            println(s"Location Info: Country -> $country")
            println(s"Sentiment: $sentiment")
            println("------------------------------------------------")

            // Prepare updated tweet JSON
            val updatedTweet = parsedTweet ++ Map(
              "cleaned_text" -> cleanedText,
              "hashed_text" -> hashedText,
              "location_info" -> Map("country" -> country),
              "sentiment" -> sentiment
            )
            val updatedTweetJson = org.json4s.jackson.Serialization.write(updatedTweet)

            // Send to Kafka output topic
            producer.send(new ProducerRecord[String, String](outputTopic, updatedTweetJson))
            println("📤 Sentiment analysis sent to Kafka topic: " + outputTopic)
          } catch {
            case e: Exception =>
              println(s"❌ Error processing tweet: ${e.getMessage}")
          }
        }
      }
    } finally {
      // Close consumer and producer
      consumer.close()
      producer.close()
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

  // Analyze the sentiment based on keywords
  def analyzeSentiment(text: String, happyWords: Set[String], sadWords: Set[String], neutralWords: Set[String]): String = {
    val words = text.split(" ").map(_.trim.toLowerCase)

    val happyCount = words.count(word => happyWords.contains(word))
    val sadCount = words.count(word => sadWords.contains(word))
    val neutralCount = words.count(word => neutralWords.contains(word))

    // Determine sentiment based on the most frequent category
    if (happyCount > sadCount && happyCount > neutralCount) {
      "Happy"
    } else if (sadCount > happyCount && sadCount > neutralCount) {
      "Sad"
    } else {
      "Neutral"
    }
  }
}
