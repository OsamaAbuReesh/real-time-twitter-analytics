# Real-Time Twitter Analytics

This project implements a real-time analytics pipeline for processing Twitter data. It uses Apache Kafka, Scala, Docker, Elasticsearch, and Kibana to efficiently process and analyze tweet datasets stored locally.

---

## Features

- **Real-Time Data Ingestion**: Processes data from a JSON file containing Twitter data.
- **Scalable Processing**: Leverages Apache Kafka for handling high-throughput data streams.
- **Batch Processing**: Processes tweets in configurable batch sizes for efficiency.
- **Error Handling**: Implements robust error handling for JSON parsing and Kafka message delivery.
- **Dockerized Setup**: Simplifies environment setup using Docker and Docker Compose.

---

## Prerequisites

- **Scala** 2.12 or later
- **Java** 8 or later
- **Docker** and **Docker Compose**
- **Apache Kafka** and **Zookeeper**
- **Elasticsearch** and **Kibana**

---

## The Pipline 
![image](https://github.com/user-attachments/assets/db9df135-4272-4171-b2b4-8b0fe26706cc)


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
1. Elasticsearch Port & API (The data is sent to Elasticsearch on port `http://localhost:9200`  using the REST API)
2. Kafka Integration (Consumed tweets from Kafka topic `user_analysis_topic`)
3. Data Indexing (Indexed tweets into Elasticsearch index:` tweets_analysis_000`)
4. Authentication (Used Elasticsearch Basic Authentication for secure access)


### **7. Visualize Data in Kibana**

1. Open Kibana in your browser (default: `http://localhost:5601`).
2. Create an index pattern for the Elasticsearch index (e.g., `tweets_analysis_000`).
3. Build visualizations and dashboards:

   - **Sentiment Analysis**: Pie chart showing sentiment distribution.
   - **Global Tweet Activity (Map)**: Displays geospatial distribution of tweets using a world map.
   - **Tweet Activity Over Time**: A line chart showing tweet frequency over time for specific periods
   - **Hourly Tweet Activity**:A bar chart visualizing tweet activity distributed by hours of the day.
   - **Sentiment Distribution**:A bar chart representing the proportion of positive, negative, and neutral tweets.
   - **Influential Users**:Shows the count of tweets by whether the user is influential.
   - **Sentiment Analysis Visualization**:Displays the count of tweets categorized by sentiment scores.
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
  "created_at": "Tue Dec 31 18:49:31 +0000 2013",
  "id": 418091565161017345,
  "id_str": "418091565161017345",
  "text": "@WeatherDude17 Not that revved up yet due to model inconsistency. I'd say 0-2\" w/ a decent chance of >1\" #snow #COwx #weather #Denver",
  "truncated": false,
  "entities": {
    "hashtags": [
      { "text": "snow", "indices": [108, 113] },
      { "text": "COwx", "indices": [114, 119] },
      { "text": "weather", "indices": [120, 128] },
      { "text": "Denver", "indices": [129, 136] }
    ],
    "symbols": [],
    "user_mentions": [
      {
        "screen_name": "WeatherDude17",
        "name": "WeatherDude",
        "id": 1214463582,
        "id_str": "1214463582",
        "indices": [0, 14]
      }
    ],
    "urls": []
  },
  "source": "<a href=\"https://about.twitter.com/products/tweetdeck\" rel=\"nofollow\">TweetDeck</a>",
  "in_reply_to_status_id": 418091408994471937,
  "in_reply_to_status_id_str": "418091408994471937",
  "in_reply_to_user_id": 1214463582,
  "in_reply_to_user_id_str": "1214463582",
  "in_reply_to_screen_name": "WeatherDude17",
  "user": {
    "id": 164856599,
    "id_str": "164856599",
    "name": "Josh Larson",
    "screen_name": "coloradowx",
    "location": "Denver, CO",
    "description": "Bringing you weather information & forecasts for the Denver metro area and Colorado. Previously worked at NOAA's CPC & @capitalweather.",
    "url": "https://t.co/TFT5G0nnPh",
    "entities": {
      "url": {
        "urls": [
          {
            "url": "https://t.co/TFT5G0nnPh",
            "expanded_url": "http://www.weather5280.com",
            "display_url": "weather5280.com",
            "indices": [0, 23]
          }
        ]
      },
      "description": { "urls": [] }
    },
    "protected": false,
    "followers_count": 2181,
    "friends_count": 458,
    "listed_count": 199,
    "created_at": "Fri Jul 09 23:15:25 +0000 2010",
    "favourites_count": 14777,
    "utc_offset": -25200,
    "time_zone": "Mountain Time (US & Canada)",
    "geo_enabled": true,
    "verified": false,
    "statuses_count": 18024,
    "lang": "en",
    "profile_background_color": "C0DEED",
    "profile_background_image_url": "http://abs.twimg.com/images/themes/theme1/bg.png",
    "profile_background_image_url_https": "https://abs.twimg.com/images/themes/theme1/bg.png",
    "profile_background_tile": false,
    "profile_image_url": "http://pbs.twimg.com/profile_images/910542678072238082/DYfwLSOF_normal.jpg",
    "profile_image_url_https": "https://pbs.twimg.com/profile_images/910542678072238082/DYfwLSOF_normal.jpg",
    "profile_link_color": "1DA1F2",
    "profile_sidebar_border_color": "C0DEED",
    "profile_sidebar_fill_color": "DDEEF6",
    "profile_text_color": "333333",
    "profile_use_background_image": true,
    "default_profile": true,
    "default_profile_image": false
  },
  "geo": null,
  "coordinates": null,
  "place": null,
  "contributors": null,
  "is_quote_status": false,
  "retweet_count": 0,
  "favorite_count": 0,
  "favorited": false,
  "retweeted": false,
  "lang": "en"
}

```

### **Output: Processed and Enriched Data**

```json
{
  "_index": "tweets_analysis_000",
  "_id": "QyW2LZQBLB3O4tb__bLx",
  "_version": 1,
  "_score": 0,
  "_source": {
    "user": {
      "name": "John",
      "screen_name": "constrictornews",
      "followers_count": 340,
      "id": "130934604",
      "is_influential": false
    },
    "tweet": {
      "timestamp": "2014-01-01T06:59:33Z",
      "text": "Jax Fish House Boulder",
      "id": "N/A",
      "text_hashed": "c45afd7766cd946b7213470433959be4e5be78434d53822c55abccdc964662d0",
      "sentiment_score": 0,
      "geo": {
        "lat": 40.01718,
        "lon": -105.283146
      },
      "sentiment": "Neutral"
    }
  },
  "fields": {
    "tweet.geo": [
      {
        "coordinates": [
          -105.283146,
          40.01718
        ],
        "type": "Point"
      }
    ],
    "user.is_influential": [
      false
    ],
    "user.id": [
      "130934604"
    ],
    "tweet.sentiment_score": [
      0
    ],
    "tweet.text.keyword": [
      "Jax Fish House Boulder"
    ],
    "user.name": [
      "John"
    ],
    "tweet.text_hashed": [
      "c45afd7766cd946b7213470433959be4e5be78434d53822c55abccdc964662d0"
    ],
    "tweet.text_hashed.keyword": [
      "c45afd7766cd946b7213470433959be4e5be78434d53822c55abccdc964662d0"
    ],
    "user.id.keyword": [
      "130934604"
    ],
    "tweet.timestamp": [
      "2014-01-01T06:59:33.000Z"
    ],
    "tweet.id": [
      "N/A"
    ],
    "user.screen_name.keyword": [
      "constrictornews"
    ],
    "tweet.id.keyword": [
      "N/A"
    ],
    "tweet.sentiment.keyword": [
      "Neutral"
    ],
    "user.screen_name": [
      "constrictornews"
    ],
    "tweet.text": [
      "Jax Fish House Boulder"
    ],
    "tweet.sentiment": [
      "Neutral"
    ],
    "user.name.keyword": [
      "John"
    ],
    "user.followers_count": [
      340
    ]
  }
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

- **Osama AbuReesh**: Structure & Kafka Producer
- **Toqa Asedah**: Elasticsearch Integration & Influencer Analysis
- **Ruaa BaniOdeh**: Data Preprocessing
- **Roaa Kittanh**: Sentiment Analysis & Tweet Location Extraction

---

## Acknowledgment

A big thank you to all team members for their dedication and hard work on this project. Your contributions have been invaluable in making this real-time analytics pipeline a success!
