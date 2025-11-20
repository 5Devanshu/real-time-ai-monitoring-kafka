# Real-Time AI Monitoring Pipeline with Kafka

This project establishes a comprehensive real-time AI monitoring pipeline designed for ingesting, processing, and analyzing logs, alerts, and system events with sub-second latency. It leverages Apache Kafka for high-throughput messaging, PySpark for stream processing and AI-based classification, Elasticsearch for hot-tier analytics, S3 for cold-tier archival, and Streamlit for interactive real-time visualization.

## Architecture Overview
The real-time monitoring pipeline is structured around the following key components:

1.  **Log Generator (Kafka Producer):** A Python script (`log_generator.py`) reads from a `Windows_2k.log` dataset, parses each log line into a structured JSON event, and publishes these events to a Kafka topic. This acts as the entry point for raw monitoring data.
2.  **Kafka Broker:** Apache Kafka serves as a high-throughput, fault-tolerant message broker, buffering incoming log events from the producer.
3.  **PySpark Streaming Transformer:** A PySpark application (`pyspark_streaming_transformer.py`) consumes log events from Kafka. It performs real-time transformations, including parsing and enriching the data, and applies AI-based classification (e.g., anomaly detection routing).
    *   **Hot Tier Sink (Elasticsearch):** Transformed and classified events are continuously pushed to Elasticsearch, enabling rapid querying and visualization for real-time operational monitoring.
    *   **Cold Tier Sink (S3):** All processed data is also archived to an S3 bucket for cost-effective long-term storage and historical analysis.
4.  **Elasticsearch:** Stores processed log and event data, optimized for fast full-text search and analytical queries, serving the hot tier of data.
5.  **Amazon S3:** Provides durable and scalable object storage for the cold tier, ensuring all historical data is retained.
6.  **Streamlit Dashboard (Integrated with Kibana Visualization):** A Streamlit application (`streamlit_dashboard.py`) connects directly to Elasticsearch to fetch and display real-time system health metrics, event trends, and AI-classified alerts. This dashboard effectively replaces a direct Kibana interface by leveraging Elasticsearch data for visualization within a custom, interactive Python interface.

## Features:
-   **High-Throughput Log Ingestion:** `log_generator.py` efficiently reads log files (e.g., `Windows_2k.log`) and produces structured JSON events to Kafka, ensuring reliable data delivery.
-   **Real-Time Stream Processing with PySpark:** `pyspark_streaming_transformer.py` consumes data from Kafka, performing:
    *   **Data Parsing & Enrichment:** Transforms raw log strings into structured data.
    *   **AI-based Event Classification:** Integrates a placeholder for AI/ML models to classify events (e.g., "NORMAL", "ANOMALY", "CRITICAL_ANOMALY"), enabling smart alerting and routing.
    *   **Dual-Tier Storage:** Automatically routes processed data to:
        *   **Elasticsearch (Hot Tier):** For immediate search and visualization by the Streamlit dashboard.
        *   **Amazon S3 (Cold Tier):** For cost-effective archival and compliance, with data typically partitioned for efficient querying.
-   **Interactive Streamlit Dashboard:** `streamlit_dashboard.py` provides a user-friendly web interface for:
    *   Real-time system health monitoring through key metrics and charts.
    *   Filtering events by severity, service name, and event type.
    *   Visualization of event trends over time and distributions of event types/severities, mimicking Kibana's analytical capabilities.
    *   Displaying AI-classified anomalies and critical alerts.

## Technologies Used:
-   **Python:** Core language for log generation, Streamlit dashboard, and glue code.
-   **Apache Kafka:** Distributed streaming platform for building real-time data pipelines.
-   **PySpark:** Apache Spark's Python API for scalable stream processing and transformations.
-   **Elasticsearch:** Distributed, RESTful search and analytics engine for hot data storage and fast queries.
-   **Amazon S3:** Object storage service for archiving cold-tier data.
-   **Streamlit:** Python library for creating interactive web applications for data visualization.
-   **Kibana (conceptual integration):** While not directly deployed, its visualization capabilities are replicated within the Streamlit dashboard by querying Elasticsearch.
-   **`kafka-python`:** Python client for Kafka producer.
-   **`pyspark` with `spark-sql-kafka-0-10`, `elasticsearch-spark` and `hadoop-aws` packages:** For Kafka integration, Elasticsearch sink, and S3 sink in PySpark.
-   **`elasticsearch` (Python client):** For Streamlit to interact with Elasticsearch.

## Setup and Deployment:
1.  **Environment Setup:**
    *   Install Java, Apache Spark, Python 3.x.
    *   Install Python dependencies: `pip install kafka-python elasticsearch pandas streamlit`
    *   Ensure Docker is installed for easy setup of Kafka and Elasticsearch.
2.  **Kafka Setup:**
    *   Start a Kafka broker (e.g., using Docker Compose with Zookeeper and Kafka).
    *   Create a Kafka topic (e.g., `monitoring_logs`).
3.  **Elasticsearch Setup:**
    *   Start an Elasticsearch instance (e.g., using Docker). Ensure it's accessible on `localhost:9200` and configure `ES_USER`, `ES_PASSWORD` (e.g., `elastic` and `changeme` by default in Docker setups).
    *   No manual index creation needed, PySpark will create `monitoring_data`.
4.  **S3 Setup:**
    *   Create an S3 bucket for cold storage (e.g., `your-cold-storage-bucket`).
    *   Configure AWS credentials for your Spark environment (e.g., via environment variables `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` or Spark configurations for `s3a`).
5.  **Run Components:**
    *   **Start Log Generator:** `python real-time-ai-monitoring-kafka/log_generator.py` (ensure `Windows_2k.log` is in the same directory or provide full path).
    *   **Start PySpark Streaming Transformer:**
        ```bash
        spark-submit --packages \
        org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,\
        org.elasticsearch:elasticsearch-spark-30_2.12:7.15.0,\
        com.amazonaws:aws-java-sdk-bundle:1.11.901,\
        org.apache.hadoop:hadoop-aws:3.2.0 \
        real-time-ai-monitoring-kafka/pyspark_streaming_transformer.py
        ```
    *   **Start Streamlit Dashboard:** `streamlit run real-time-ai-monitoring-kafka/streamlit_dashboard.py`

## GitHub:
