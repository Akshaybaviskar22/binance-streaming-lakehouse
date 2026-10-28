# Bronze Stream Job

# 1. IMPORTS (stdlib → third-party → local)
import os, sys, argparse, shutil
from dotenv import load_dotenv

# 2. ENV SETUP (before pyspark import)
load_dotenv()
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME", "")
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ.get("PATH", "")

# 3. PYSPARK IMPORTS
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from schema import crypto_trades_schema

# 4. ARGS PARSING
parser = argparse.ArgumentParser()
parser.add_argument("--full-refresh", action="store_true", default=False)
args = parser.parse_args()

# 5. CONFIG / CONSTANTS
BASE_PATH = os.environ["LAKE_BASE_PATH"]
KAFKA_SERVERS = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
KAFKA_STARTING_OFFSETS = os.environ["KAFKA_STARTING_OFFSETS"]
BRONZE_PATH = f"{BASE_PATH}/bronze/trades"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints/bronze/trades"

# 6. FULL REFRESH (before SparkSession — delete old data first)
if args.full_refresh:
    shutil.rmtree(BRONZE_PATH, ignore_errors=True)
    shutil.rmtree(CHECKPOINT_PATH, ignore_errors=True)
    print("Full refresh: deleted Bronze data and checkpoint")

# 7. SPARK SESSION
spark = (
    SparkSession.builder.appName("crypto-stream")
    .config(
        "spark.jars.packages",
        "io.delta:delta-spark_2.13:4.4.0,"
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0"
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.caseSensitive", "true")
    .getOrCreate()
)

# 8. READ (source)
input_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_SERVERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", KAFKA_STARTING_OFFSETS)
    .load()
)

# 9. TRANSFORM (parse raw JSON → flat columns, keep Kafka metadata for lineage)
# NOTE: Delta enforces case-insensitive column uniqueness, so the Binance
# case-colliding pairs (e/E, t/T, m/M) get explicit safe names here.
# Values stay raw (strings/longs) — cleaning/casting is Silver's job.
input_parsed_df = (
    input_df
    .withColumn("json_str", F.from_json(F.col("value").cast("string"), crypto_trades_schema))
    .select(
        F.col("json_str.e").alias("event_type"),
        F.col("json_str.E").alias("event_time_ms"),
        F.col("json_str.s").alias("symbol"),
        F.col("json_str.t").alias("trade_id"),
        F.col("json_str.p").alias("price"),
        F.col("json_str.q").alias("quantity"),
        F.col("json_str.T").alias("trade_time_ms"),
        F.col("json_str.m").alias("is_buyer_maker"),
        F.col("json_str.M").alias("is_best_match"),
        F.col("timestamp").alias("ingest_ts_ms"),
        F.col("offset").alias("kafka_offset"),
        F.col("partition").alias("kafka_partition"),
        F.col("topic").alias("kafka_topic"),
    )
)

# 10. WRITE (sink)
query = (
    input_parsed_df.writeStream
    .format("delta")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

# 11. AWAIT
query.awaitTermination()
