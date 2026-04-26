"""
Spark Structured Streaming job — real-time order processing pipeline.

Architecture (Medallion):
  Kafka topic: orders
      │
      ▼
  [Bronze] raw_orders   → Delta Lake (append)
      │
      ▼
  [Silver] aggregated   → Delta Lake (update) — windowed revenue per category
"""

import os

# Ensure PySpark can find the JVM when JAVA_HOME is not set in the shell
if not os.environ.get("JAVA_HOME"):
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window,
    sum as _sum, count, avg, max as _max,
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType,
)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DELTA_BASE = os.path.join(BASE_DIR, "delta")

RAW_PATH  = os.path.join(DELTA_BASE, "raw_orders")
AGG_PATH  = os.path.join(DELTA_BASE, "aggregated")
CKPT_RAW  = os.path.join(DELTA_BASE, "checkpoints", "raw_orders")
CKPT_AGG  = os.path.join(DELTA_BASE, "checkpoints", "aggregated")

KAFKA_SERVERS = "localhost:9092"
KAFKA_TOPIC   = "orders"

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("OrderStreamProcessor")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
        "io.delta:delta-spark_2.12:3.1.0",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # keep shuffle partitions low for local development
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------
ORDER_SCHEMA = StructType([
    StructField("order_id",  StringType(),  nullable=False),
    StructField("user_id",   StringType(),  nullable=True),
    StructField("product",   StringType(),  nullable=True),
    StructField("category",  StringType(),  nullable=True),
    StructField("amount",    DoubleType(),  nullable=True),
    StructField("quantity",  IntegerType(), nullable=True),
    StructField("status",    StringType(),  nullable=True),
    StructField("timestamp", StringType(),  nullable=True),
])

# ---------------------------------------------------------------------------
# Source — read from Kafka
# ---------------------------------------------------------------------------
kafka_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

# Parse JSON payload
orders = (
    kafka_raw
    .select(
        from_json(col("value").cast("string"), ORDER_SCHEMA).alias("d"),
        col("timestamp").alias("kafka_ingest_time"),
    )
    .select("d.*", "kafka_ingest_time")
    .withColumn("event_time", to_timestamp(col("timestamp")))
    .filter(col("order_id").isNotNull())
)

# ---------------------------------------------------------------------------
# Bronze sink — raw orders (append)
# ---------------------------------------------------------------------------
bronze_query = (
    orders.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CKPT_RAW)
    .trigger(processingTime="10 seconds")
    .start(RAW_PATH)
)

# ---------------------------------------------------------------------------
# Silver — windowed aggregation (revenue per category per 1-minute window)
# ---------------------------------------------------------------------------
revenue_by_category = (
    orders
    .withWatermark("event_time", "2 minutes")
    .groupBy(
        window(col("event_time"), "1 minute"),
        col("category"),
    )
    .agg(
        _sum("amount").alias("total_revenue"),
        count("order_id").alias("order_count"),
        avg("amount").alias("avg_order_value"),
        _max("amount").alias("max_order_value"),
        _sum("quantity").alias("total_units_sold"),
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("category"),
        col("total_revenue"),
        col("order_count"),
        col("avg_order_value"),
        col("max_order_value"),
        col("total_units_sold"),
    )
)

silver_query = (
    revenue_by_category.writeStream
    .format("delta")
    .outputMode("append")   # Delta only supports append for streaming agg with watermark
    .option("checkpointLocation", CKPT_AGG)
    .trigger(processingTime="10 seconds")
    .start(AGG_PATH)
)

# ---------------------------------------------------------------------------
# Console sink — live preview in terminal
# ---------------------------------------------------------------------------
console_query = (
    revenue_by_category.writeStream
    .format("console")
    .outputMode("update")
    .option("truncate", False)
    .option("numRows", 20)
    .trigger(processingTime="15 seconds")
    .start()
)

# ---------------------------------------------------------------------------
# Run until interrupted
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("  Order Stream Processor running.")
print(f"  Bronze (raw)  → {RAW_PATH}")
print(f"  Silver (agg)  → {AGG_PATH}")
print("  Console sink active. Press Ctrl+C to stop.")
print("=" * 60 + "\n")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\nShutting down streaming queries...")
    for q in spark.streams.active:
        q.stop()
    print("All queries stopped.")
