"""
Streamlit real-time dashboard — reads from Delta Lake tables written by
order_processor.py and auto-refreshes every N seconds.

Run:
    streamlit run dashboard/app.py
"""

import os
import time
from datetime import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from deltalake import DeltaTable

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Order Stream Dashboard",
    page_icon="📦",
    layout="wide",
    initial_sidebar_state="expanded",
)

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_PATH = os.path.join(BASE_DIR, "delta", "raw_orders")
AGG_PATH = os.path.join(BASE_DIR, "delta", "aggregated")

CATEGORY_COLORS = px.colors.qualitative.Bold

# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
with st.sidebar:
    st.title("⚙️ Controls")
    refresh_sec = st.slider("Refresh interval (s)", min_value=3, max_value=30, value=5)
    max_recent  = st.slider("Recent orders to show", min_value=5, max_value=100, value=20)

    st.divider()
    st.markdown("**Pipeline links**")
    st.markdown("- Kafka UI → [localhost:8080](http://localhost:8080)")

    st.divider()
    st.markdown("**Stack**")
    st.markdown(
        "Kafka · PySpark Structured Streaming · Delta Lake · Streamlit"
    )

# ---------------------------------------------------------------------------
# Data loaders  (ttl=2 avoids re-reading on every widget interaction)
# ---------------------------------------------------------------------------
@st.cache_data(ttl=2)
def load_raw() -> pd.DataFrame:
    if not os.path.exists(RAW_PATH):
        return pd.DataFrame()
    try:
        df = DeltaTable(RAW_PATH).to_pandas()
        df["event_time"] = pd.to_datetime(df["timestamp"])
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=2)
def load_agg() -> pd.DataFrame:
    if not os.path.exists(AGG_PATH):
        return pd.DataFrame()
    try:
        df = DeltaTable(AGG_PATH).to_pandas()
        df["window_start"] = pd.to_datetime(df["window_start"])
        df["window_end"]   = pd.to_datetime(df["window_end"])
        return df
    except Exception:
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("📦 Real-Time Order Stream Dashboard")
st.caption(
    f"Last updated: {datetime.now().strftime('%H:%M:%S')}  ·  "
    f"Auto-refresh every {refresh_sec}s  ·  Source: Delta Lake (Bronze + Silver)"
)

raw_df = load_raw()
agg_df = load_agg()

# ---------------------------------------------------------------------------
# Empty state — pipeline not started yet
# ---------------------------------------------------------------------------
if raw_df.empty:
    st.warning(
        "No data yet. Start the pipeline first:\n\n"
        "```bash\n"
        "python streaming/order_processor.py   # terminal 1\n"
        "python producer/order_producer.py      # terminal 2\n"
        "```"
    )
    time.sleep(refresh_sec)
    st.rerun()

# ---------------------------------------------------------------------------
# KPI row
# ---------------------------------------------------------------------------
total_revenue      = raw_df["amount"].sum()
total_orders       = len(raw_df)
avg_order_value    = raw_df["amount"].mean()
active_categories  = raw_df["category"].nunique()

k1, k2, k3, k4 = st.columns(4)
k1.metric("💰 Total Revenue",      f"${total_revenue:,.2f}")
k2.metric("🛒 Total Orders",       f"{total_orders:,}")
k3.metric("📊 Avg Order Value",    f"${avg_order_value:.2f}")
k4.metric("🏷️ Active Categories",  str(active_categories))

st.divider()

# ---------------------------------------------------------------------------
# Row 2 — Revenue by category  |  Revenue over time
# ---------------------------------------------------------------------------
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Revenue by Category")
    cat_df = (
        raw_df.groupby("category")["amount"]
        .sum()
        .reset_index()
        .sort_values("amount", ascending=True)
        .rename(columns={"amount": "total_revenue"})
    )
    fig_bar = px.bar(
        cat_df,
        x="total_revenue", y="category", orientation="h",
        color="category",
        color_discrete_sequence=CATEGORY_COLORS,
        labels={"total_revenue": "Revenue ($)", "category": ""},
        text_auto=".2s",
    )
    fig_bar.update_layout(showlegend=False, margin=dict(l=0, r=10, t=10, b=0))
    st.plotly_chart(fig_bar, use_container_width=True)

with col_right:
    st.subheader("Revenue Over Time (1-min windows)")
    if not agg_df.empty:
        time_df = (
            agg_df.groupby("window_start")["total_revenue"]
            .sum()
            .reset_index()
            .sort_values("window_start")
        )
        fig_line = px.line(
            time_df, x="window_start", y="total_revenue",
            markers=True,
            color_discrete_sequence=["#00CC96"],
            labels={"window_start": "Window Start", "total_revenue": "Revenue ($)"},
        )
        fig_line.update_layout(margin=dict(l=0, r=10, t=10, b=0))
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.info("Waiting for the first aggregation window (~1 min of data needed).")

st.divider()

# ---------------------------------------------------------------------------
# Row 3 — Order status breakdown  |  Order value distribution
# ---------------------------------------------------------------------------
col_l2, col_r2 = st.columns(2)

with col_l2:
    st.subheader("Orders by Status")
    status_df = (
        raw_df["status"].value_counts()
        .reset_index()
        .rename(columns={"count": "orders"})
    )
    fig_pie = px.pie(
        status_df, names="status", values="orders",
        color_discrete_sequence=px.colors.qualitative.Pastel,
        hole=0.4,
    )
    fig_pie.update_layout(margin=dict(l=0, r=0, t=10, b=0))
    st.plotly_chart(fig_pie, use_container_width=True)

with col_r2:
    st.subheader("Order Value Distribution")
    fig_hist = px.histogram(
        raw_df, x="amount", nbins=30, color="category",
        color_discrete_sequence=CATEGORY_COLORS,
        labels={"amount": "Order Value ($)"},
        opacity=0.8,
    )
    fig_hist.update_layout(
        showlegend=True,
        legend=dict(orientation="h", y=-0.2),
        margin=dict(l=0, r=10, t=10, b=0),
    )
    st.plotly_chart(fig_hist, use_container_width=True)

st.divider()

# ---------------------------------------------------------------------------
# Row 4 — Live aggregation table (Silver layer)
# ---------------------------------------------------------------------------
if not agg_df.empty:
    st.subheader("Windowed Aggregations — Silver Layer")
    latest_agg = (
        agg_df
        .sort_values("window_start", ascending=False)
        .head(30)
        .reset_index(drop=True)
    )
    latest_agg["total_revenue"]   = latest_agg["total_revenue"].map("${:,.2f}".format)
    latest_agg["avg_order_value"] = latest_agg["avg_order_value"].map("${:,.2f}".format)
    latest_agg["max_order_value"] = latest_agg["max_order_value"].map("${:,.2f}".format)
    st.dataframe(latest_agg, use_container_width=True)
    st.divider()

# ---------------------------------------------------------------------------
# Row 5 — Recent raw orders (Bronze layer)
# ---------------------------------------------------------------------------
st.subheader(f"Recent {max_recent} Orders — Bronze Layer (Raw Events)")
recent = (
    raw_df
    .sort_values("event_time", ascending=False)
    .head(max_recent)
    [["order_id", "product", "category", "amount", "quantity", "status", "event_time"]]
    .reset_index(drop=True)
)
st.dataframe(recent, use_container_width=True)

# ---------------------------------------------------------------------------
# Auto-refresh
# ---------------------------------------------------------------------------
time.sleep(refresh_sec)
st.rerun()
