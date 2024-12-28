import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import edu.stanford.nlp.pipeline._
import edu.stanford.nlp.ling._
import edu.stanford.nlp.sentiment._
import edu.stanford.nlp.util._
import scala.jdk.CollectionConverters._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import java.util.Properties
import java.time.Duration

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

    // Create the NLP pipeline
    val pipeline = createNLPipeline()

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
            val text = parsedTweet.get("text") match {
              case Some(t: String) => t
              case _ => "N/A"
            }

            // Analyze sentiment
            val sentiment = analyzeSentiment(text, pipeline)
            println(s"🔧 Sentiment: $sentiment")

            // Update the tweet with sentiment
            val updatedTweet = parsedTweet + ("sentiment" -> sentiment)
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

  // Create Stanford NLP pipeline
  def createNLPipeline(): StanfordCoreNLP = {
    val props = new Properties()
    props.setProperty("annotators", "tokenize,ssplit,pos,lemma,parse,sentiment")
    new StanfordCoreNLP(props)
  }

  // Analyze the sentiment of the given text
  def analyzeSentiment(text: String, pipeline: StanfordCoreNLP): String = {
    val document = new Annotation(text)
    pipeline.annotate(document)

    val sentencesJavaList = document.get(classOf[CoreAnnotations.SentencesAnnotation])
    val sentences = sentencesJavaList.asScala
    val sentiments = sentences.map { sentence =>
      sentence.get(classOf[SentimentCoreAnnotations.SentimentClass])
    }

    // Determine the most common sentiment
    if (sentiments.nonEmpty) {
      sentiments.groupBy(identity).mapValues(_.size).maxBy(_._2)._1
    } else {
      "Neutral"
    }
  }
}
