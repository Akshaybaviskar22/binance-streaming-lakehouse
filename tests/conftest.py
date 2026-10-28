import os
import sys
import time

# make src/ importable (transforms.py) regardless of where pytest runs from
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from dotenv import load_dotenv

# mirror the env setup the jobs use (finds local Java)
load_dotenv()
os.environ["JAVA_HOME"] = os.environ.get("JAVA_HOME", "")
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ.get("PATH", "")

# pin workers to the same interpreter as the driver (venv python)
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# deterministic timestamps: collected values render in the driver's local tz,
# so pin it to UTC (must happen BEFORE the JVM / any datetime is created)
os.environ["TZ"] = "UTC"
time.tzset()

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("transforms-tests")
        .config("spark.jars.packages", "io.delta:delta-spark_2.13:4.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
