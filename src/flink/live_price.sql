-- Flink SQL — live price & volume (hot path).
--
-- Run via Flink SQL client (see README § Flink):
--   docker exec -it capstone-flink-jobmanager /opt/flink/bin/sql-client.sh
--   SQL> SET 'execution.runtime-mode' = 'streaming';
--   SQL> SOURCE '/sql/live_price.sql';
--
-- In Docker network use kafka:9092, on host use localhost:9092.

-- Source: raw Binance trades from Kafka (JSON, raw field names)
CREATE TABLE IF NOT EXISTS trades (
  `e` STRING,
  `E` BIGINT,
  `s` STRING,
  `t` BIGINT,
  `p` STRING,
  `q` STRING,
  `T` BIGINT,
  `m` BOOLEAN,
  `M` BOOLEAN,
  trade_time AS TO_TIMESTAMP_LTZ(`T`, 3),
  WATERMARK FOR trade_time AS trade_time - INTERVAL '5' SECONDS
) WITH (
  'connector' = 'kafka',
  'topic' = 'crypto.trades',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-live-price',
  'scan.startup.mode' = 'latest-offset',
  'format' = 'json'
);

-- Sink: 1-second live price/volume back to Kafka (dashboard / Grafana can subscribe)
CREATE TABLE IF NOT EXISTS live_price (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  avg_price DOUBLE,
  high DOUBLE,
  low DOUBLE,
  volume DOUBLE,
  trade_count BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'crypto.price.live',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

-- 1-second tumbling window → live price
INSERT INTO live_price
SELECT
  window_start,
  window_end,
  AVG(CAST(p AS DOUBLE)) AS avg_price,
  MAX(CAST(p AS DOUBLE)) AS high,
  MIN(CAST(p AS DOUBLE)) AS low,
  SUM(CAST(q AS DOUBLE)) AS volume,
  COUNT(*) AS trade_count
FROM TABLE(
  TUMBLE(TABLE trades, DESCRIPTOR(trade_time), INTERVAL '1' SECOND)
)
GROUP BY window_start, window_end;

-- Optional: 1-minute volume spike alert (uncomment to enable)
-- CREATE TABLE IF NOT EXISTS volume_alerts (
--   window_start TIMESTAMP(3), volume DOUBLE, trade_count BIGINT
-- ) WITH ('connector' = 'kafka', 'topic' = 'crypto.volume.alerts',
--         'properties.bootstrap.servers' = 'kafka:9092', 'format' = 'json');
--
-- INSERT INTO volume_alerts
-- SELECT window_start, SUM(CAST(q AS DOUBLE)) AS volume, COUNT(*) AS trade_count
-- FROM TABLE(TUMBLE(TABLE trades, DESCRIPTOR(trade_time), INTERVAL '1' MINUTE))
-- GROUP BY window_start
-- HAVING SUM(CAST(q AS DOUBLE)) > 10.0;
