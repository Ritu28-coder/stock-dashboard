import streamlit as st
import pandas as pd
import snowflake.connector
import plotly.graph_objects as go
import plotly.express as px


# ──────────────────────────────────────────────────────────────
# 🎯  STREAMLIT SETUP
# ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="📈 Stock Dashboard", layout="wide")
st.markdown(
    "<h1 style='text-align:center;'>📈 Real‑Time Stock Dashboard</h1>",
    unsafe_allow_html=True,
)
st.markdown(
     "<h3 style='text-align:center;'>Track Prices, Volume & Sectors in Seconds</h>",
    unsafe_allow_html=True,
)

# Add this just below your `st.set_page_config(...)` line
st.markdown("""
    <style>
    [data-baseweb="tab-list"] button {
        font-size: 1.2rem !important;
        padding: 0.75rem 1.5rem !important;
    }
    </style>
""", unsafe_allow_html=True)

st.markdown(
    """
    <style>
    /* Make tab headers larger and bolder */
    .stTabs [data-baseweb="tab-list"] {
        justify-content: left;
        gap: 2rem;
        font-size: 1.1rem;
    }

    .stTabs [data-baseweb="tab"] {
        padding: 0.75rem 1.5rem;
        border-radius: 0.5rem 0.5rem 0 0;
        background-color: #1e1e1e;
        color: white;
        font-weight: 500;
        transition: background-color 0.3s ease;
    }

    .stTabs [aria-selected="true"] {
        background-color: #262730;
        color: #00ffe7;
    }
    </style>
    """,
    unsafe_allow_html=True
)



# ──────────────────────────────────────────────────────────────
# ❄️  SNOWFLAKE CONNECTION (using secrets)
# ──────────────────────────────────────────────────────────────
creds = st.secrets["snowflake"]
conn = snowflake.connector.connect(**creds)

query = """
SELECT ticker, date, close_price, volume, sector
FROM StockData
WHERE date >= DATEADD(day, -14, CURRENT_DATE)   -- pull last 14 days
ORDER BY date DESC
"""
df = pd.read_sql(query, conn)
conn.close()

df["DATE"] = pd.to_datetime(df["DATE"])

# ──────────────────────────────────────────────────────────────
# 🎛️  SIDEBAR — ALL CONTROLS
# ──────────────────────────────────────────────────────────────
st.sidebar.markdown("## 🎛️ Controls Panel")

if st.sidebar.button("🔄 Refresh Now", key="refresh_button"):
    st.rerun()

# 0️⃣ Base min/max dates (before any filter)
min_date  = df["DATE"].min().date()
max_date  = df["DATE"].max().date()

# 1️⃣ Date + Sector filters
with st.sidebar.expander("📅 Date + Sector Filters", expanded=True):
    date_range = st.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )
    if not isinstance(date_range, (list, tuple)) or len(date_range) != 2:
        st.warning("⚠️ Pick a start *and* end date."); st.stop()

    start_date, end_date = date_range
    df = df[(df["DATE"] >= pd.to_datetime(start_date)) &
            (df["DATE"] <= pd.to_datetime(end_date))]

    df.columns = [c.upper() for c in df.columns]  # normalize
    sectors = sorted(df["SECTOR"].dropna().unique())
    selected_sector = st.selectbox("🏢 Sector", ["All"] + sectors)
    if selected_sector != "All":
        df = df[df["SECTOR"] == selected_sector]

# 2️⃣ Price‑range slider
with st.sidebar.expander("💲 Price Range", expanded=False):
    p_min, p_max = float(df["CLOSE_PRICE"].min()), float(df["CLOSE_PRICE"].max())
    low, high = st.slider(
        "Show stocks between … and … USD",
        min_value=p_min, max_value=p_max,
        value=(p_min, p_max), step=1.0,
    )
    df = df[(df["CLOSE_PRICE"] >= low) & (df["CLOSE_PRICE"] <= high)]

# 3️⃣ Ticker selector (multi‑select)
with st.sidebar.expander("🔍 Stock Selection", expanded=True):
    tickers = sorted(df["TICKER"].unique())
    multi = st.multiselect("Compare multiple tickers", options=tickers,
                           default=[tickers[0]], key="multi_tickers")
    filtered_df = df[df["TICKER"].isin(multi)]

# 4️⃣ Chart style
with st.sidebar.expander("🎨 Chart Style", expanded=False):
    chart_type = st.radio("Choose chart type", ["Line", "Area"], horizontal=True)

# 5️⃣ About
st.sidebar.subheader('📘 About')
st.sidebar.markdown("""
This dashboard is built with **Streamlit + Snowflake** to visualize real-time stock data.

🔗 [GitHub Repository](https://github.com/Ritu28-coder/stock-dashboard)
""")


# ──────────────────────────────────────────────────────────────
# 📊  MAIN TABS
# ──────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(
    ["📈 Chart", "📊 Volume & Movers", "📄 Data / Export", "📊 Sector Pie"]
)

# ── TAB 1 • metrics first, then single‑ticker chart ──────────
with tab1:

    if not multi:
        st.info("👉 Select at least one ticker to view chart and metrics.")
        st.stop()

    # Use the first selected ticker for detailed view
    sym = multi[0]
    sym_df = filtered_df[filtered_df["TICKER"] == sym]

    if sym_df.empty:
        st.warning(f"No data for {sym} in the current filters.")
        st.stop()

    # ── Metrics block (top)
    latest = sym_df.sort_values("DATE").iloc[-1]["CLOSE_PRICE"]
    first  = sym_df.sort_values("DATE").iloc[0]["CLOSE_PRICE"]
    delta  = latest - first
    pct    = delta / first * 100
    high   = sym_df["CLOSE_PRICE"].max()
    low    = sym_df["CLOSE_PRICE"].min()
    vol    = sym_df["VOLUME"].sum()

    delta_color = "inverse" if delta > 0 else "off" if delta < 0 else "normal"

    m1, m2, m3 = st.columns(3)
    m1.metric("Last Close", f"${latest:.2f}",
              delta=f"{delta:+.2f} ({pct:+.2f} %)", delta_color=delta_color)
    m2.metric("High", f"${high:.2f}")
    m3.metric("Low",  f"${low:.2f}")
    st.metric("Volume", f"{vol:,}")

    st.markdown("---")

    # ── Chart (below metrics)
    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=sym_df["DATE"],
            y=sym_df["CLOSE_PRICE"],
            mode="lines" if chart_type == "Line" else "lines",
            name=sym,
            stackgroup="one" if chart_type == "Area" else None,
        )
    )
    fig.update_layout(
        title=f"{sym} Price Trend",
        xaxis_title="Date",
        yaxis_title="Price (USD)",
        height=500,
    )
    st.plotly_chart(fig, use_container_width=True)


# ── TAB 2 • volume & movers ──────────────────────────────────
with tab2:
    st.subheader("📊 Top 10 by Volume")
    top_vol = df.groupby("TICKER")["VOLUME"].sum().sort_values(ascending=False).head(10)
    st.bar_chart(top_vol)

    st.subheader("📈 Top 5 Gainers & 📉 Losers")
    change_pct = df.groupby("TICKER").apply(
        lambda x: (x.sort_values("DATE").iloc[-1]["CLOSE_PRICE"] -
                   x.sort_values("DATE").iloc[0]["CLOSE_PRICE"]) /
                  x.sort_values("DATE").iloc[0]["CLOSE_PRICE"] * 100
    )
    gainers = change_pct.sort_values(ascending=False).head(5)
    losers  = change_pct.sort_values().head(5)

    cg1, cg2 = st.columns(2)
    cg1.bar_chart(gainers)
    cg1.markdown("#### 🟢 Gainers")
    cg2.bar_chart(losers)
    cg2.markdown("#### 🔴 Losers")

with tab3:
    st.subheader("📄 Filtered Raw Data")
    st.dataframe(filtered_df, use_container_width=True)
    st.info("➡️ Use the **︙** menu at top‑right of the table to download CSV.")  # optional helper text


# ── TAB 4 • sector pie ───────────────────────────────────────
with tab4:
    st.subheader("📊 Volume Distribution by Sector")
    sec_tot = df.groupby("SECTOR")["VOLUME"].sum()
    if not sec_tot.empty:
        fig_pie = px.pie(
            values=sec_tot.values,
            names=sec_tot.index,
            hole=0.4,
            title="Total Volume by Sector"
        )
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("Sector data unavailable for current filters.")
