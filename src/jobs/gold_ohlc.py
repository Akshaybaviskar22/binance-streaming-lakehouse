# Gold OHLC Job — silver → gold (hourly OHLCV candles, batch)

# 1. IMPORTS (stdlib → third-party → local)
import os, sys, argparse, shutil
from dotenv import load_dotenv

# 2. ENV SETUP (before pyspark import)
load_dotenv()
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

# 3. PYSPARK IMPORTS
from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from transforms import compute_ohlc

# 4. ARGS PARSING
parser = argparse.ArgumentParser()
parser.add_argument("--full-refresh", action="store_true", default=False)
args = parser.parse_args()

# 5. CONFIG / CONSTANTS
BASE_PATH = os.environ["LAKE_BASE_PATH"]
SILVER_PATH = f"{BASE_PATH}/silver/trades"
GOLD_PATH = f"{BASE_PATH}/gold/ohlc_1h"

# 6. FULL REFRESH
if args.full_refresh:
    shutil.rmtree(GOLD_PATH, ignore_errors=True)
    print("Full refresh: deleted Gold data")

# 7. SPARK SESSION
spark = (
    SparkSession.builder.appName("crypto-gold")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.4.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.caseSensitive", "true")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# 8. READ (source — silver Delta table)
silver_df = spark.read.format("delta").load(SILVER_PATH)

if silver_df.isEmpty():
    print("Silver is empty — run the producer + bronze + silver first. Nothing to do.")
    spark.stop()
    raise SystemExit(0)

# 9. TRANSFORM (hourly OHLCV candles)
ohlc_df = compute_ohlc(silver_df)

# 10. WRITE (MERGE on window_start → idempotent re-runs, upserts the current hour)
if not DeltaTable.isDeltaTable(spark, GOLD_PATH):
    ohlc_df.write.format("delta").partitionBy("trade_date").save(GOLD_PATH)
else:
    (
        DeltaTable.forPath(spark, GOLD_PATH).alias("target")
        .merge(ohlc_df.alias("src"), "target.window_start = src.window_start")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

row_count = ohlc_df.count()
print(f"Gold OHLC written → {GOLD_PATH} ({row_count} hourly candles)")
spark.stop()
