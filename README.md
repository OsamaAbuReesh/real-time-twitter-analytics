# Real-Time Twitter Analytics

This project is a real-time analytics pipeline for processing Twitter data.

---

## **1. Build the Docker Image**

To build the Docker image, run the following command in your project root directory:
 
```bash
docker build -t real-time-twitter-analytics .
docker run -p 8000:8000 real-time-twitter-analytics
```

## **Key Commands Used**

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
