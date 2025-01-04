# Real-Time Twitter Analytics

This project implements a real-time analytics pipeline for processing Twitter data. It uses Apache Kafka, Scala, Docker, Elasticsearch, and Kibana to efficiently process and analyze tweet datasets stored locally.

---

## Features

- **Real-Time Data Ingestion**: Processes data from a JSON file containing Twitter data.
- **Scalable Processing**: Leverages Apache Kafka for handling high-throughput data streams.
- **Batch Processing**: Processes tweets in configurable batch sizes for efficiency.
- **Error Handling**: Implements robust error handling for JSON parsing and Kafka message delivery.
- **Logging**: Provides detailed logging for monitoring and debugging.
- **Dockerized Setup**: Simplifies environment setup using Docker and Docker Compose.

---

## Prerequisites

- **Scala** 2.12 or later
- **Java** 8 or later
- **Docker** and **Docker Compose**
- **Apache Kafka** and **Zookeeper**
- **Elasticsearch** and **Kibana**

---

## Setup Instructions

### 1. Clone the Repository

```bash
git clone https://github.com/OsamaAbuReesh/real-time-twitter-analytics.git
cd real-time-twitter-analytics

### 2. Build the Docker Image

To build and run the Docker image:

```bash
docker build -t real-time-twitter-analytics .
docker run -p 8000:8000 real-time-twitter-analytics
```

### 3. Start Required Services

Start Kafka, Zookeeper, Elasticsearch, and Kibana using Docker Compose:

```bash
docker-compose up -d
```

Verify the services are running:

```bash
docker ps
```

### 4. Build and Run the Scala Application

#### Build the Scala Project
Compile the Scala application using SBT:
```bash
sbt compile
```

#### Run the Kafka Producer
Start ingesting tweets into the Kafka topic `tweets_topic`:
```bash
sbt runMain KafkaProducerApp
```

---

## Workflow Steps

### **1. Data Cleaning**

Clean the raw tweet data by removing invalid or incomplete records:
```bash
sbt runMain CleaningConsumerApp
```

### **2. Tweet Feature Extraction**

Extract features such as hashtags, mentions, and URLs:
```bash
sbt runMain TweetFeatureExtractionConsumer
```

### **3. Tweet Location Extraction**

Extract geolocation data from tweets, if available:
```bash
sbt runMain TweetLocationConsumer
```

### **4. Sentiment Analysis**

Analyze tweets for positive, negative, or neutral sentiment:
```bash
sbt runMain SentimentAnalysisConsumer
```

### **5. Influencer Analysis**

Identify influential users based on retweets, replies, and followers:
```bash
sbt runMain InfluencerAnalysisConsumer
```

### **6. Send Data to Elasticsearch**

Store processed tweets in Elasticsearch for querying and visualization:
```bash
sbt runMain KafkaToElasticsearchConsumer
```

### **7. Visualize Data in Kibana**

1. Open Kibana in your browser (default: `http://localhost:5601`).
2. Create an index pattern for the Elasticsearch index (e.g., `tweets-index`).
3. Build visualizations and dashboards:
   - **Sentiment Analysis**: Pie chart showing sentiment distribution.
   - **Time Series**: Trends of tweet volume over time.
   - **Geospatial Data**: Map of tweet locations.
   - **Influencer Metrics**: Top users by retweets or followers.

---

## Key Docker Commands

| **Action**                        | **Command**                                                                                           |
|-----------------------------------|-------------------------------------------------------------------------------------------------------|
| **Build Docker Image**            | `docker build -t real-time-twitter-analytics .`                                                      |
| **Run Docker Container**          | `docker run -p 8000:8000 real-time-twitter-analytics`                                                |
| **Start Docker Compose**          | `docker-compose up -d`                                                                               |
| **Stop Docker Compose**           | `docker-compose down`                                                                                |
| **Access Kafka Container**        | `docker exec -it real-time-twitter-analytics-kafka-1 bash`                                           |
| **List Kafka Topics**             | `/opt/bitnami/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092`                    |
| **Delete a Kafka Topic**          | `/opt/bitnami/kafka/bin/kafka-topics.sh --delete --topic <topic-name> --bootstrap-server localhost:9092` |
| **Create a Kafka Topic**          | `/opt/bitnami/kafka/bin/kafka-topics.sh --create --topic tweets_topic --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1` |
| **Consume Kafka Messages**        | `/opt/bitnami/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic tweets_topic --from-beginning` |

---

## Example Input and Output

### **Input: Raw JSON Tweet**

```json
{
  "id": "12345",
  "text": "This is a test tweet",
  "user": {
    "id": "67890",
    "name": "John Doe",
    "followers_count": 500
  },
  "coordinates": [40.7128, -74.0060],
  "timestamp": "2025-01-04T12:34:56Z"
}
```

### **Output: Processed and Enriched Data**

```json
{
  "id": "12345",
  "text": "This is a test tweet",
  "user": "John Doe",
  "followers_count": 500,
  "location": [40.7128, -74.0060],
  "sentiment": "neutral",
  "hashtags": [],
  "mentions": [],
  "timestamp": "2025-01-04T12:34:56Z"
}
```

---

## Monitoring and Troubleshooting

### **Common Issues**

#### Kafka Connection Refused
- **Cause**: Kafka or Zookeeper is not running.
- **Solution**:
  1. Ensure Docker containers are running:
     ```bash
     docker ps
     ```
  2. Restart Docker Compose:
     ```bash
     docker-compose down
     docker-compose up -d
     ```

#### Elasticsearch Not Receiving Data
- **Cause**: Issues with the Kafka-to-Elasticsearch consumer.
- **Solution**:
  1. Check the Kafka topic for data:
     ```bash
     docker exec -it kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic tweets_topic --from-beginning
     ```
  2. Verify Elasticsearch logs for errors:
     ```bash
     docker logs elasticsearch
     ```
## Team Members

- **Osama AbuReesh**: Structure and Kafka Producer
- **Toqa Asedah**: Elasticsearch Integration
- **Ruaa BaniOdeh**: Data Preprocessing
- **Roaa Kittanh**: Sentiment Analysis

---

## Acknowledgment

A big thank you to all team members for their dedication and hard work on this project. Your contributions have been invaluable in making this real-time analytics pipeline a success!
