#!/usr/bin/env python3
"""
Streamlit dashboard — live BTC/USDT ticker (Kafka) + historical OHLC candles (Delta gold).

Run: make dashboard   (or: streamlit run dashboard/app.py)
"""

import json
import os
import threading
from collections import deque

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()

LAKE_BASE_PATH = os.environ.get("LAKE_BASE_PATH", "data/lake")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "crypto.trades")
GOLD_PATH = f"{LAKE_BASE_PATH}/gold/ohlc_1h"

st.set_page_config(page_title="Crypto Market Platform", page_icon="📈", layout="wide")


# ---------------------------------------------------------------- live ticker
def start_live_consumer():
    """Background thread: Kafka consumer filling a rolling in-memory buffer."""
    if "live_trades" not in st.session_state:
        buffer = deque(maxlen=2000)

        def _consume():
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP],
                auto_offset_reset="latest",
                value_deserializer=lambda m: json.loads(m.decode()),
            )
            for msg in consumer:
                buffer.append(msg.value)

        threading.Thread(target=_consume, daemon=True).start()
        st.session_state.live_trades = buffer
    return st.session_state.live_trades


# ---------------------------------------------------------------- gold loader
@st.cache_data(ttl=60)
def load_gold() -> pd.DataFrame:
    from deltalake import DeltaTable

    return DeltaTable(GOLD_PATH).to_pandas()


# ---------------------------------------------------------------- page
st.title("📈 Live Crypto Market Platform")
st.caption("Binance WebSocket → Kafka → Spark Delta lakehouse · Flink hot path")

with st.sidebar:
    st.header("Pipeline health")
    st.markdown(
        f"""
        - **Kafka**: `{KAFKA_BOOTSTRAP}` / `{KAFKA_TOPIC}`
        - **Gold table**: `{GOLD_PATH}`
        """
    )
    st.markdown(
        "Start the pipeline with `make up producer run-bronze run-silver run-gold`"
    )

# --- Live section (Kafka) ------------------------------------------------
st.subheader("⚡ Live BTC/USDT")

try:
    from streamlit_autorefresh import st_autorefresh

    st_autorefresh(interval=2000, limit=None, key="live_refresh")
except ImportError:
    st.info("Install dashboard deps for auto-refresh: `make dashboard-deps`")

buffer = start_live_consumer()
live_df = pd.DataFrame(list(buffer))

if live_df.empty:
    st.info("Waiting for live trades — is the producer running? (`make producer`)")
else:
    live_df["price"] = live_df["p"].astype(float)
    live_df["qty"] = live_df["q"].astype(float)
    last = live_df.iloc[-1]

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Last price", f"${last['price']:,.2f}")
    if len(live_df) > 1:
        delta = last["price"] - live_df["price"].iloc[-2]
        c2.metric("Change (tick)", f"{delta:+,.2f}")
    c3.metric("Trades in buffer", f"{len(live_df):,}")
    c4.metric("1-min volume (approx)", f"{live_df['qty'].tail(60).sum():,.4f} BTC")

    st.line_chart(live_df["price"].tail(300).reset_index(drop=True))

# --- Historical section (Delta gold) -------------------------------------
st.subheader("🕯️ Historical OHLC (1h candles from gold)")

try:
    gold = load_gold()
except Exception:
    st.info("Gold table not found yet — run `make run-gold` once silver has data.")
else:
    gold = gold.sort_values("window_start")
    dates = sorted(gold["trade_date"].unique(), reverse=True)

    picked = st.multiselect("Dates", dates, default=dates[:1] if dates else [])
    view = gold[gold["trade_date"].isin(picked)] if picked else gold.tail(48)

    st.write(
        f"{len(view)} candles · latest close "
        f"**${view['close'].iloc[-1]:,.2f}**"
        if not view.empty
        else "No candles for selection."
    )

    if not view.empty:
        import plotly.graph_objects as go

        fig = go.Figure(
            data=[
                go.Candlestick(
                    x=view["window_start"],
                    open=view["open"],
                    high=view["high"],
                    low=view["low"],
                    close=view["close"],
                )
            ]
        )
        fig.update_layout(
            height=450,
            xaxis_rangeslider_visible=False,
            margin=dict(l=10, r=10, t=10, b=10),
        )
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Gold table (raw rows)"):
            st.dataframe(view)
