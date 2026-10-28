# Run targets for the crypto market data platform.
# Usage: make <target>   (see README for the full walkthrough)

PY ?= python
RUN = PYTHONPATH=src $(PY)

.PHONY: up down producer consumer \
        run-bronze run-silver run-gold \
        refresh-bronze refresh-silver refresh-gold \
        test dashboard dashboard-deps \
        full-up full-down flink-up flink-down

# --- infrastructure ---
up:
	cd docker && docker compose up -d

down:
	cd docker && docker compose down

# --- pipeline (host mode: Kafka in Docker, jobs on your Mac) ---
producer:
	$(PY) scripts/kafka_producer.py

consumer:
	$(PY) scripts/kafka_consumer.py

run-bronze:
	$(RUN) src/jobs/bronze_stream.py

run-silver:
	$(RUN) src/jobs/silver_job.py

run-gold:
	$(RUN) src/jobs/gold_ohlc.py

refresh-bronze:
	$(RUN) src/jobs/bronze_stream.py --full-refresh

refresh-silver:
	$(RUN) src/jobs/silver_job.py --full-refresh

refresh-gold:
	$(RUN) src/jobs/gold_ohlc.py --full-refresh

# --- quality / demo ---
test:
	$(PY) -m pytest tests/ -v

dashboard-deps:
	$(PY) -m pip install -r dashboard/requirements.txt

dashboard:
	$(PY) -m streamlit run dashboard/app.py

# --- full-stack docker mode ---
full-up:
	cd docker && docker compose --profile full up -d --build

full-down:
	cd docker && docker compose --profile full down

flink-up:
	cd docker && docker compose --profile flink up -d

flink-down:
	cd docker && docker compose --profile flink down
