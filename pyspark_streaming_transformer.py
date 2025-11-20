from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, lit
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType, MapType

def create_spark_session(app_name="KafkaStreamProcessing"):
    """
    Creates and configures a SparkSession for Kafka streaming.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.0,com.amazonaws:aws-java-sdk-bundle:1.11.901,org.apache.hadoop:hadoop-aws:3.2.0") \
        .config("spark.driver.extraClassPath", "/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar:/opt/spark/jars/hadoop-aws-3.2.0.jar") \
        .config("spark.executor.extraClassPath", "/opt/spark/jars/aws-java-sdk-bundle-1.11.901.jar:/opt/spark/jars/hadoop-aws-3.2.0.jar") \
        .config("spark.es.nodes", "localhost") \
        .config("spark.es.port", "9200") \
        .config("spark.es.net.http.auth.user", "elastic") \
        .config("spark.es.net.http.auth.pass", "changeme") \
        .config("spark.es.nodes.wan.only", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

def define_schema():
    """
    Defines the schema for the incoming Kafka messages (log events).
    """
    return StructType([
        StructField("timestamp", StringType()),
        StructField("event_id", StringType()),
        StructField("event_type", StringType()),
        StructField("service_name", StringType()),
        StructField("severity", StringType()),
        StructField("message", StringType()),
        StructField("details", MapType(StringType(), StringType())) # Details can be complex, use MapType for flexibility
    ])

def process_kafka_stream(spark, kafka_bootstrap_servers='localhost:9092', kafka_topic='monitoring_logs',
                         es_nodes='localhost:9200', es_index='monitoring_data',
                         s3_cold_storage_path='s3a://your-cold-storage-bucket/monitoring_logs_cold/'):
    """
    Reads from Kafka, transforms data using PySpark, and sinks to Elasticsearch (hot tier)
    and S3 (cold tier).
    """
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()

    # Define schema for JSON parsing
    schema = define_schema()

    # Parse JSON value and select relevant fields
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json_value") \
        .withColumn("data", from_json(col("json_value"), schema)) \
        .select(
            col("data.timestamp").cast(TimestampType()).alias("timestamp"),
            col("data.event_id").alias("event_id"),
            col("data.event_type").alias("event_type"),
            col("data.service_name").alias("service_name"),
            col("data.severity").alias("severity"),
            col("data.message").alias("message"),
            col("data.details").alias("details_map"),
            current_timestamp().alias("processing_time")
        )

    # Further transformations, e.g., flattening details map for Elasticsearch indexing
    # This assumes `details` map contains simple key-value pairs that can be directly mapped
    # For more complex nested structures, additional flattening might be needed.
    transformed_df = parsed_df \
        .withColumn("cpu_usage", col("details_map.cpu_usage").cast(DoubleType())) \
        .withColumn("memory_usage_mb", col("details_map.memory_usage_mb").cast(IntegerType())) \
        .withColumn("disk_io_rate", col("details_map.disk_io_rate").cast(DoubleType())) \
        .withColumn("network_latency_ms", col("details_map.network_latency_ms").cast(IntegerType())) \
        .withColumn("user_id", col("details_map.user_id").cast(StringType())) \
        .drop("details_map")

    # AI-based event classification (placeholder for actual ML model integration)
    # For demonstration, let's add a dummy classification score and category
    transformed_df = transformed_df \
        .withColumn("ai_score", lit(random.uniform(0.1, 0.9)).cast(DoubleType())) \
        .withColumn("ai_category", lit(random.choice(["NORMAL", "ANOMALY", "CRITICAL_ANOMALY"])).cast(StringType()))

    # Sink to Elasticsearch (Hot Tier)
    # Ensure Elasticsearch is running and accessible. Index will be created automatically.
    es_query = transformed_df \
        .writeStream \
        .format("org.elasticsearch.spark.sql") \
        .option("checkpointLocation", "/tmp/checkpoints/es_sink") \
        .option("es.resource", es_index) \
        .option("es.nodes", es_nodes.split(':')[0]) \
        .option("es.port", es_nodes.split(':')[1] if ':' in es_nodes else "9200") \
        .option("es.net.http.auth.user", "elastic") \
        .option("es.net.http.auth.pass", "changeme") \
        .outputMode("append") \
        .start()

    # Sink to S3 (Cold Tier)
    # Ensure your S3 bucket exists and Spark has write permissions.
    # Data will be partitioned by date for efficient cold storage.
    s3_query = transformed_df \
        .writeStream \
        .format("parquet") \
        .option("checkpointLocation", "/tmp/checkpoints/s3_sink") \
        .option("path", s3_cold_storage_path) \
        .partitionBy("service_name", "event_type") \
        .outputMode("append") \
        .start()

    print("PySpark Streaming started. Ingesting from Kafka, transforming, and sinking to Elasticsearch and S3.")

    # Await termination of the queries
    es_query.awaitTermination()
    s3_query.awaitTermination()

if __name__ == "__main__":
    # To run this script:
    # 1. Install Java and Spark.
    # 2. Install kafka-python, elasticsearch, boto3.
    # 3. Ensure Kafka is running on localhost:9092.
    # 4. Ensure Elasticsearch is running on localhost:9200.
    # 5. Create an S3 bucket for cold storage (e.g., 'your-cold-storage-bucket').
    # 6. Configure AWS credentials for Spark to write to S3.
    #    This often involves setting AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    #    environment variables or configuring Spark with s3a properties.
    # 7. Submit with spark-submit:
    #    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0,org.elasticsearch:elasticsearch-spark-30_2.12:7.15.0 real-time-ai-monitoring-kafka/pyspark_streaming_transformer.py

    print("Starting PySpark Streaming Transformer...")
    spark_session = create_spark_session()
    process_kafka_stream(spark_session)
    spark_session.stop()
    print("PySpark Streaming Transformer stopped.")
