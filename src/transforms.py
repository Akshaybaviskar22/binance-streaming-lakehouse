# Shared transformations — Silver cleaning + Gold OHLC logic.
# Pure DataFrame functions so jobs and tests reuse the same code.

from pyspark.sql import functions as F


def to_silver(df):
    """Bronze (raw Binance values, Delta-safe names) → Silver (clean, typed, validated trades).

    - numeric casts (try_cast tolerates malformed payloads → dropped by validation)
    - validation: trade_id/symbol present, price & quantity positive
    - Kafka metadata kept for lineage
    """
    return (
        df.select(
            F.col("trade_id"),
            F.col("symbol"),
            F.expr("TRY_CAST(price AS DOUBLE)").alias("price"),
            F.expr("TRY_CAST(quantity AS DOUBLE)").alias("quantity"),
            (F.col("trade_time_ms") / 1000).cast("timestamp").alias("trade_time"),
            F.col("event_type"),
            (F.col("event_time_ms") / 1000).cast("timestamp").alias("event_time"),
            F.col("is_buyer_maker"),
            F.col("is_best_match"),
            (F.col("ingest_ts_ms") / 1000).cast("timestamp").alias("ingest_ts"),
            F.col("kafka_offset"),
            F.col("kafka_partition"),
            F.col("kafka_topic"),
        )
        .where(
            F.col("trade_id").isNotNull()
            & F.col("symbol").isNotNull()
            & (F.col("price") > 0)
            & (F.col("quantity") > 0)
        )
    )


def compute_ohlc(df):
    """Silver trades → 1-hour OHLCV candles.

    open/close use first/last trade by trade_time (struct trick),
    high/low/volume/trade_count are plain aggregations.
    """
    return (
        df.groupBy(F.window(F.col("trade_time"), "1 hour").alias("w"))
        .agg(
            F.min(F.struct("trade_time", "price")).getField("price").alias("open"),
            F.max(F.struct("trade_time", "price")).getField("price").alias("close"),
            F.max("price").alias("high"),
            F.min("price").alias("low"),
            F.sum("quantity").alias("volume"),
            F.count(F.lit(1)).alias("trade_count"),
        )
        .select(
            F.col("w.start").alias("window_start"),
            F.col("w.end").alias("window_end"),
            F.col("open"),
            F.col("high"),
            F.col("low"),
            F.col("close"),
            F.col("volume"),
            F.col("trade_count"),
            F.to_date(F.col("w.start")).alias("trade_date"),
        )
    )
