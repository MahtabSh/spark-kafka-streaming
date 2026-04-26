"""
Interactive query script — read processed results from Delta Lake tables.
Run after order_processor.py has been active for at least one minute.

Usage:
    python streaming/query_delta.py
"""

import os

if not os.environ.get("JAVA_HOME"):
    os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc

BASE_DIR  = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PATH  = os.path.join(BASE_DIR, "delta", "raw_orders")
AGG_PATH  = os.path.join(BASE_DIR, "delta", "aggregated")

spark = (
    SparkSession.builder
    .appName("DeltaQueryLayer")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.12:3.1.0",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .master("local[*]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ---------------------------------------------------------------------------
# 1. Raw order count
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("BRONZE — Total raw orders ingested")
print("=" * 60)
raw_df = spark.read.format("delta").load(RAW_PATH)
print(f"Total records: {raw_df.count()}")
raw_df.show(10, truncate=False)

# ---------------------------------------------------------------------------
# 2. Revenue by category (all windows combined)
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("SILVER — Revenue by category (all time)")
print("=" * 60)
agg_df = spark.read.format("delta").load(AGG_PATH)
(
    agg_df
    .groupBy("category")
    .agg(
        {"total_revenue": "sum", "order_count": "sum", "avg_order_value": "avg"}
    )
    .withColumnRenamed("sum(total_revenue)", "total_revenue")
    .withColumnRenamed("sum(order_count)", "total_orders")
    .withColumnRenamed("avg(avg_order_value)", "avg_order_value")
    .orderBy(desc("total_revenue"))
    .show(truncate=False)
)

# ---------------------------------------------------------------------------
# 3. Latest window per category
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("SILVER — Latest 1-minute window per category")
print("=" * 60)
agg_df.orderBy(desc("window_start")).show(20, truncate=False)

# ---------------------------------------------------------------------------
# 4. Delta table history
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
print("DELTA — Bronze table history")
print("=" * 60)
spark.sql(f"DESCRIBE HISTORY delta.`{RAW_PATH}`").show(5, truncate=False)

spark.stop()
