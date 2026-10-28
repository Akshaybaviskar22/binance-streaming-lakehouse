# Setup Guide

Get the pipeline running from a fresh clone/unzip in 5 steps.

## Prerequisites

| Tool | Version | Check | Install (macOS) |
|---|---|---|---|
| Python | 3.11+ | `python3 --version` | `brew install python` |
| Java | 17+ | `java -version` | `brew install openjdk@17` |
| Docker | any | `docker --version` | `brew install --cask docker` (or OrbStack) |

## 1. Create the virtual environment

```bash
cd streaming-lakehouse-capstone
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
```

## 2. Install dependencies

```bash
pip install -r requirements.txt
```

> ~800MB on disk — PySpark bundles the full Spark engine. This is normal.

## 3. Configure

```bash
cp .env.example .env
```

Edit `.env` and set:

```ini
LAKE_BASE_PATH=/absolute/path/to/streaming-lakehouse-capstone/data/lake
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=crypto.trades
KAFKA_STARTING_OFFSETS=latest
JAVA_HOME=/path/to/java/home     # e.g. $(/usr/libexec/java_home -v 17)
```

## 4. Run the pipeline

```bash
make up            # Kafka (Docker)
make producer      # Binance WebSocket → Kafka   (terminal 1, leave running)
make run-bronze    # Kafka → Delta bronze        (terminal 2, leave running)
make run-silver    # bronze → silver (MERGE)     (terminal 3, leave running)
make run-gold      # silver → gold OHLC candles  (re-run anytime)
```

## 5. Verify

```bash
make test          # pytest suite on transformations
make dashboard-deps && make dashboard   # http://localhost:8501
```

## Troubleshooting

- **`PYTHON_VERSION_MISMATCH`** — always run jobs with the venv active (`source .venv/bin/activate`). The jobs pin `PYSPARK_PYTHON` automatically.
- **Kafka unreachable** — `docker compose up -d` must run from `docker/` (or use `make up`) and Docker Desktop must be running.
- **Empty gold table** — silver needs a minute of trades; re-run `make run-gold` after silver has data.
- **Full reset** — `make refresh-bronze refresh-silver refresh-gold` wipes tables + checkpoints and rebuilds.
