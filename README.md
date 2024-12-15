# Real-Time Twitter Analytics

This project is a real-time analytics pipeline for processing Twitter data.

---

## **1. Build the Docker Image**

To build the Docker image, run the following command in your project root directory:
 
```bash
docker build -t real-time-twitter-analytics .
docker run -p 8000:8000 real-time-twitter-analytics

