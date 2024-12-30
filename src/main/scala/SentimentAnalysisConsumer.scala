import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.util.Properties
import java.time.Duration
import java.security.MessageDigest
import scala.jdk.CollectionConverters._

// Object for sentiment analysis using Kafka
object SentimentAnalysisConsumer {
  def main(args: Array[String]): Unit = {
    val inputTopic = "location_tweets_topic"
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

    try {
      while (true) {
        // Poll for new records
        val records = consumer.poll(Duration.ofMillis(100)).iterator()
        for (record <- records.asScala) {
          val cleanedTweet = record.value()
          println("🚀 Received cleaned tweet:")
          println(cleanedTweet)

          try {
            // Parse the tweet
            val parsedTweet = parse(cleanedTweet).extract[Map[String, Any]]

            // Extract the cleaned text for analysis
            val cleanedText = parsedTweet.get("cleaned_text") match {
              case Some(text: String) => text
              case _ => ""
            }

            // Generate hash of the cleaned text
            val textHashed = computeHash(cleanedText)

            // Analyze sentiment
            val sentiment = analyzeSentiment(cleanedText, happyWords, sadWords, neutralWords)
            println(s"🔧 Sentiment: $sentiment")

            // Update the tweet with sentiment and hash
            val updatedTweet = parsedTweet + ("sentiment" -> sentiment) + ("text_hashed" -> textHashed)
            val updatedTweetJson = org.json4s.jackson.Serialization.write(updatedTweet)

            // Send updated tweet to the output Kafka topic
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

  // Compute hash of a given text using SHA-256
  def computeHash(data: String): String = {
    val md = MessageDigest.getInstance("SHA-256")
    val hashBytes = md.digest(data.getBytes("UTF-8"))
    hashBytes.map("%02x".format(_)).mkString
  }
}
