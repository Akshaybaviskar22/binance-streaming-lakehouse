"""Tests for the shared silver + gold transformations."""

from datetime import datetime, timezone

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from transforms import compute_ohlc, to_silver

BRONZE_SCHEMA = StructType(
    [
        StructField("event_type", StringType()),
        StructField("event_time_ms", LongType()),
        StructField("symbol", StringType()),
        StructField("trade_id", LongType()),
        StructField("price", StringType()),
        StructField("quantity", StringType()),
        StructField("trade_time_ms", LongType()),
        StructField("is_buyer_maker", BooleanType()),
        StructField("is_best_match", BooleanType()),
        StructField("ingest_ts_ms", LongType()),
        StructField("kafka_offset", LongType()),
        StructField("kafka_partition", LongType()),
        StructField("kafka_topic", StringType()),
    ]
)


def ts_ms(year, month, day, hour, minute):
    return int(
        datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp() * 1000
    )


def bronze_row(t, p, q, T, s="BTCUSDT", m=False, M=True, offset=0):
    return {
        "event_type": "trade",
        "event_time_ms": T,
        "symbol": s,
        "trade_id": t,
        "price": p,
        "quantity": q,
        "trade_time_ms": T,
        "is_buyer_maker": m,
        "is_best_match": M,
        "ingest_ts_ms": T,
        "kafka_offset": offset,
        "kafka_partition": 0,
        "kafka_topic": "crypto.trades",
    }


def bronze_df(spark, rows):
    return spark.createDataFrame(rows, schema=BRONZE_SCHEMA)


# ------------------------------------------------------------------ silver


def test_to_silver_renames_and_casts(spark):
    df = bronze_df(spark, [bronze_row(1, "42000.50", "0.25", ts_ms(2026, 1, 1, 10, 5))])
    silver = to_silver(df)

    assert silver.columns == [
        "trade_id",
        "symbol",
        "price",
        "quantity",
        "trade_time",
        "event_type",
        "event_time",
        "is_buyer_maker",
        "is_best_match",
        "ingest_ts",
        "kafka_offset",
        "kafka_partition",
        "kafka_topic",
    ]

    row = silver.first()
    assert row["trade_id"] == 1
    assert row["symbol"] == "BTCUSDT"
    assert row["price"] == 42000.50
    assert row["quantity"] == 0.25
    assert str(row["trade_time"]).startswith("2026-01-01 10:05")


def test_to_silver_drops_invalid_rows(spark):
    rows = [
        bronze_row(1, "42000.50", "0.25", ts_ms(2026, 1, 1, 10, 5)),  # valid
        bronze_row(2, "0", "0.25", ts_ms(2026, 1, 1, 10, 6)),  # price not > 0
        bronze_row(3, "-5", "0.25", ts_ms(2026, 1, 1, 10, 7)),  # negative price
        bronze_row(4, "42000.50", "0", ts_ms(2026, 1, 1, 10, 8)),  # qty not > 0
        bronze_row(5, "42000.50", "0.25", ts_ms(2026, 1, 1, 10, 9), s=None),  # no symbol
        bronze_row(6, "not-a-number", "0.25", ts_ms(2026, 1, 1, 10, 10)),  # unparseable price → null
    ]
    silver = to_silver(bronze_df(spark, rows))

    assert silver.count() == 1
    assert silver.first()["trade_id"] == 1


# ------------------------------------------------------------------ gold


def test_compute_ohlc_single_hour(spark):
    rows = [
        bronze_row(1, "100", "0.5", ts_ms(2026, 1, 1, 10, 5)),
        bronze_row(2, "110", "1.0", ts_ms(2026, 1, 1, 10, 20)),
        bronze_row(3, "90", "2.0", ts_ms(2026, 1, 1, 10, 40)),
    ]
    gold = compute_ohlc(to_silver(bronze_df(spark, rows)))

    assert gold.count() == 1
    row = gold.first()
    assert row["open"] == 100.0
    assert row["high"] == 110.0
    assert row["low"] == 90.0
    assert row["close"] == 90.0
    assert row["volume"] == 3.5
    assert row["trade_count"] == 3
    assert str(row["window_start"]).startswith("2026-01-01 10:00")
    assert str(row["trade_date"]).startswith("2026-01-01")


def test_compute_ohlc_separate_hours(spark):
    rows = [
        bronze_row(1, "100", "0.5", ts_ms(2026, 1, 1, 10, 5)),
        bronze_row(2, "110", "1.0", ts_ms(2026, 1, 1, 10, 20)),
        bronze_row(3, "95", "2.0", ts_ms(2026, 1, 1, 11, 15)),
    ]
    gold = compute_ohlc(to_silver(bronze_df(spark, rows)))

    result = {str(r["window_start"]): r for r in gold.collect()}
    assert len(result) == 2

    h10 = next(r for k, r in result.items() if k.startswith("2026-01-01 10"))
    assert (h10["open"], h10["close"], h10["trade_count"]) == (100.0, 110.0, 2)

    h11 = next(r for k, r in result.items() if k.startswith("2026-01-01 11"))
    assert (h11["open"], h11["close"], h11["trade_count"]) == (95.0, 95.0, 1)


def test_compute_ohlc_excludes_invalid_trades(spark):
    rows = [
        bronze_row(1, "100", "0.5", ts_ms(2026, 1, 1, 10, 5)),
        bronze_row(2, "-1", "0.5", ts_ms(2026, 1, 1, 10, 6)),  # dropped by silver validation
    ]
    gold = compute_ohlc(to_silver(bronze_df(spark, rows)))

    row = gold.first()
    assert row["trade_count"] == 1
    assert row["low"] == 100.0
