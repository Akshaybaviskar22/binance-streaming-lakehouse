# 📈 Live Crypto Market Data Platform

A containerized streaming lakehouse that ingests **live Binance BTC/USDT trades**, serves **real-time price/volume** via Flink, and builds a **bronze → silver → gold Delta lakehouse** with Spark for audit-grade history and OHLC analytics.

**Binance WebSocket → Kafka → Flink (hot) + Spark/Delta (lake) → Streamlit dashboard**

---

## Architecture

```text
Binance WebSocket (live BTC trades)
        │
        ▼
  kafka_producer.py
        │
        ▼
  Kafka: crypto.trades
        │
   ┌────┴────┐
   ▼         ▼
 FLINK     SPARK
 (hot)     (lake)
   │         ├── bronze/trades   (raw append + Kafka metadata)
 live KPIs   ├── silver/trades   (validated, deduped via MERGE)
   │         └── gold/ohlc_1h    (hourly OHLCV candles, MERGE upsert)
   ▼
Streamlit dashboard  (live ticker + historical candlestick charts)
```

| Path | Engine | Responsibility |
|---|---|---|
| **Hot** | Flink SQL | 1-second tumbling windows → live price/volume to `crypto.price.live` |
| **Lake** | Spark Structured Streaming + Delta | Raw audit trail, deduped trades, OHLC candles for reporting |

---

## What this project demonstrates

| Concern | Implementation |
|---|---|
| **Streaming ingestion** | Kafka (KRaft, no Zookeeper), exactly-once-friendly offsets, checkpointed Spark streams |
| **Medallion architecture** | bronze (raw + lineage) → silver (validated, typed) → gold (business aggregates) |
| **Incremental upserts** | `foreachBatch` + Delta `MERGE` on `trade_id` (silver) and `window_start` (gold) — idempotent re-runs |
| **Data quality** | Silver validation (price/quantity > 0, symbol present), in-batch dedup |
| **Time-series analytics** | Hourly OHLCV candles via windowed aggregation (first/last trade by struct trick) |
| **Dual latency paths** | Flink for sub-second KPIs, Spark for replayable lake history |
| **Testing** | pytest suite on transformation logic (renames, casts, validation, OHLC correctness) |
| **Ops** | Single `Makefile`, `.env`-driven config, full-stack `docker compose` profiles, `--full-refresh` recovery |

---

## Quickstart (host mode)

Prerequisites: **Java 17+, Docker, Python 3.11+**

```bash
git clone <repo> && cd streaming-lakehouse-capstone
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env                # set LAKE_BASE_PATH + JAVA_HOME

make up                            # Kafka
make producer                      # Binance WS → Kafka (leave running)

make run-bronze                    # Kafka → Delta bronze (leave running)
make run-silver                    # bronze → silver, MERGE dedup (leave running)
make run-gold                      # silver → gold OHLC candles (batch, re-run anytime)

make dashboard-deps                # one-time: streamlit/plotly/deltalake
make dashboard                     # http://localhost:8501
```

Verify the lake directly:

```bash
$ .venv/bin/python -c "
from deltalake import DeltaTable
dt = DeltaTable('data/lake/gold/ohlc_1h')
print(dt.to_pandas().tail())"
```

## Full-stack mode (all Docker)

```bash
make full-up                       # Kafka + producer + bronze + silver (+ gold one-shot)
make run-gold                      # or recompute candles from the host
make full-down
```

## Flink hot path

```bash
cd docker && ./flink-setup.sh      # one-time: download Kafka SQL connector
make flink-up                      # JobManager UI: http://localhost:8081

docker exec -it capstone-flink-jobmanager /opt/flink/bin/sql-client.sh
# SQL> SOURCE '/sql/live_price.sql';
```

Writes 1-second price/volume windows to topic `crypto.price.live`.

## Tests

```bash
make test
```

Covers: silver renaming/casting, validation rules (bad price/qty/symbol dropped), OHLC correctness (open/high/low/close, hour bucketing, invalid-trade exclusion).

---

## Project structure

```text
├── Makefile                  # all run targets
├── docker/
│   ├── docker-compose.yml    # kafka | full | flink profiles
│   ├── Dockerfile.producer
│   ├── Dockerfile.spark
│   └── flink-setup.sh
├── scripts/
│   ├── kafka_producer.py     # Binance WebSocket → Kafka
│   └── kafka_consumer.py     # debug consumer
├── src/
│   ├── schema.py             # Binance trade StructType (bronze)
│   ├── transforms.py         # to_silver() + compute_ohlc() — shared, tested
│   ├── jobs/
│   │   ├── bronze_stream.py  # Kafka → Delta bronze (append, checkpointed)
│   │   ├── silver_job.py     # bronze → silver (foreachBatch + MERGE)
│   │   └── gold_ohlc.py      # silver → gold (hourly OHLCV, idempotent)
│   └── flink/
│       └── live_price.sql    # 1-sec live price windows
├── dashboard/
│   └── app.py                # Streamlit: live ticker + OHLC candlesticks
├── tests/
│   └── test_transforms.py
└── data/lake/                # gitignored Delta tables + checkpoints
```

## Design decisions

- **Bronze stores raw Binance values** (strings/longs, no business casting) plus Kafka metadata (offset/partition/topic) — full replay/lineage; all cleaning is silver's job. Case-colliding Binance pairs (`e`/`E`, `t`/`T`, `m`/`M`) are renamed at ingest because Delta enforces case-insensitive column uniqueness.
- **Silver dedups via MERGE on `trade_id`**, not `dropDuplicates` alone — handles Kafka redelivery *and* replayed batches.
- **Gold MERGEs on `window_start`** — re-running the job upserts the in-progress hour instead of duplicating it.
- **`--full-refresh` flags** on every job — clean recovery path for schema changes (deletes table + checkpoint).
- **30-second triggers** on streams — avoids the small-files problem in Delta.
- **Flink owns live numbers, Spark owns the lake** — no duplicated write path, each engine used where it's strongest.

## Business ↔ tech mapping (interview talking points)

| Business need | This build |
|---|---|
| "Show the live BTC price" | Flink 1-sec windows → `crypto.price.live` |
| "Historical charts for the app" | Spark gold `ohlc_1h` Delta table |
| "Audit / replay our trades" | Bronze raw append + Delta time travel |
| "Detect unusual volume" | Flink 1-min windows + threshold (provided in SQL) |
| "Don't lose data on failure" | Kafka buffer + Spark checkpoints + idempotent MERGE |
| "Reduce pipeline cost" | Trigger batching, MERGE over rewrite, partitioned gold |

## Future work

- [ ] Redis sink for Flink + Grafana live dashboard
- [ ] Alerting on volume spikes → webhook
- [ ] Airflow orchestration for gold + OPTIMIZE/Z-ORDER jobs
- [ ] CI (GitHub Actions) running the pytest suite
