TFT Metagame Analytics Pipeline 📊

A distributed Big Data pipeline for real-time Teamfight Tactics (TFT) meta analysis, implementing a Lambda Architecture with Kafka, HDFS, Spark, and MongoDB.

📖 Overview

In the stochastic environment of Teamfight Tactics, the "Meta" shifts rapidly with every patch. This project automates the discovery of optimal strategies by ingesting high-velocity match telemetry from the Riot Games API, processing it through a distributed cluster, and serving actionable insights via a web dashboard.

Key Features:

High-Velocity Ingestion: Multi-key rotation system to bypass API rate limits.

Lambda Architecture: "Bridge" service implementing batch layer persistence (Kafka → HDFS).

Distributed Computing: PySpark algorithms to calculate placement deltas ($\Delta$) and unit synergies.

Interactive Dashboard: Flask-based UI with fuzzy-search asset mapping for visualizing Meta/Apex compositions.

Architecture

The system follows a vertical microservices topology containerized via Docker:

graph TD
    A[Riot Games API] -->|JSON Stream| B(Ingestion Service)
    B -->|Producer| C{Apache Kafka}
    C -->|Consumer| D[Bridge Service]
    D -->|Batch Write| E[(HDFS DataLake)]
    E -->|Read| F[Apache Spark Engine]
    F -->|Aggregated Stats| G[(MongoDB)]
    G -->|Query| H[Flask Web App]


Project Structure

tft-analytics/
├── docker-compose.yml          # Main orchestration file
├── .env                        # Configuration & API Keys
├── ingestion/                  # Service: Riot API -> Kafka
├── bridge/                     # Service: Kafka -> HDFS (Batch Layer)
├── processing/                 # Service: Spark ETL & Aggregation
└── web_app/                    # Service: Flask Decision Support System


 Getting Started

Prerequisites

Docker Desktop (Allocated memory > 6GB recommended)

Riot Games Developer Key (Get one at developer.riotgames.com)

Installation

Clone the repository

git clone [https://github.com/yourusername/tft-analytics.git](https://github.com/yourusername/tft-analytics.git)
cd tft-analytics


Configure Environment
Create a .env file in the root directory:

RIOT_API_KEY=RGAPI-xxxxxxxx-your-api-key


Launch the Cluster

docker-compose up -d --build


Wait ~30 seconds for Kafka and Hadoop to stabilize.

Verify Services

Hadoop NameNode: http://localhost:9870

Spark Master: http://localhost:8080

Web Dashboard: http://localhost:5000

Running the Pipeline

Step 1: Start Ingestion

The ingestion service will automatically start polling match data if the API key is valid. Check logs:

docker logs -f tft-analytics_ingestion_1


Step 2: Bridge Data to HDFS

The bridge service listens to Kafka and writes batches (N=100) to HDFS to prevent small-file overhead.

docker logs -f tft-analytics_bridge_1


Step 3: Run Spark Analysis

Trigger the analytics engine to process HDFS data and populate MongoDB.

docker exec -it tft-analytics_spark-master_1 spark-submit \
  --master spark://spark-master:7077 \
  --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1 \
  /processing/spark_processor.py


Step 4: Explore Data

Visit http://localhost:5000 to view the dashboard.

Meta Comps: Stabilized strategies ($n > 1000$ games).

Explorer: Deep dive into Unit/Item/Trait synergies with placement deltas.

Tech Stack

Ingestion: Python, Requests, Token Bucket Algorithm

Streaming: Apache Kafka, Zookeeper

Storage: Hadoop HDFS (Raw), MongoDB (Processed)

Processing: Apache Spark 3.3.2 (PySpark)

Frontend: Flask, Jinja2, HTML5/CSS3

License

Distributed under the MIT License. See LICENSE for more information.

Disclaimer: This project is not endorsed by Riot Games and does not reflect the views or opinions of Riot Games or anyone officially involved in producing or managing Riot Games properties
