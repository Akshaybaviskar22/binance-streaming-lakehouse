# Silver Stream Job — bronze → silver (clean, deduped, validated trades)

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
from transforms import to_silver

# 4. ARGS PARSING
parser = argparse.ArgumentParser()
parser.add_argument("--full-refresh", action="store_true", default=False)
args = parser.parse_args()

# 5. CONFIG / CONSTANTS
BASE_PATH = os.environ["LAKE_BASE_PATH"]
BRONZE_PATH = f"{BASE_PATH}/bronze/trades"
SILVER_PATH = f"{BASE_PATH}/silver/trades"
CHECKPOINT_PATH = f"{BASE_PATH}/checkpoints/silver/trades"

# 6. FULL REFRESH (before SparkSession — delete old data first)
if args.full_refresh:
    shutil.rmtree(SILVER_PATH, ignore_errors=True)
    shutil.rmtree(CHECKPOINT_PATH, ignore_errors=True)
    print("Full refresh: deleted Silver data and checkpoint")

# 7. SPARK SESSION
spark = (
    SparkSession.builder.appName("crypto-silver")
    .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.4.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.caseSensitive", "true")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

# 8. READ (source — bronze Delta stream)
input_df = spark.readStream.format("delta").load(BRONZE_PATH)


# 9. TRANSFORM + WRITE (foreachBatch → MERGE, dedup on trade_id)
def upsert_batch(df, batch_id):
    batch = to_silver(df).dropDuplicates(["trade_id"])

    if not DeltaTable.isDeltaTable(spark, SILVER_PATH):
        batch.write.format("delta").save(SILVER_PATH)
        return

    (
        DeltaTable.forPath(spark, SILVER_PATH).alias("target")
        .merge(batch.alias("src"), "target.trade_id = src.trade_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


query = (
    input_df.writeStream
    .foreachBatch(upsert_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="30 seconds")
    .start()
)

# 10. AWAIT
query.awaitTermination()
