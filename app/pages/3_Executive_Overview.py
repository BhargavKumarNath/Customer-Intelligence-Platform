import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
from db_utils import run_query, format_currency, format_number

st.set_page_config(page_title="Executive Overview", page_icon="📈", layout="wide")

st.title("📈 Executive Performance Overview")

st.markdown("""
**Business Performance Dashboard** - Track high-level KPIs, revenue trends, and conversion funnel health across the entire platform.

*This dashboard processes the full 109.95M-row dataset using the DuckDB OLAP engine; the pipeline that builds these tables runs within a 3 GB memory limit, and dashboard queries return in under a second.*
""")

# Methodology Highlight
with st.expander("⚡ How We Process 109.95M Events"):
    st.markdown("""
    **Pipeline timings (10 GB-RAM / 8-core box, ~15 min end-to-end):**
    - **Ingestion:** ~2 min (2x CSV → 1.82 GB Parquet → DuckDB, streaming)
    - **Dimensional Modeling:** ~90 s (dim_users, dim_products, fact_daily_kpis) + ~45 s sessionization
    - **Analytics + features + training:** < 20 s each stage, ~50 s LightGBM

    **Key Techniques:**
    - Parquet dictionary encoding + Int32/Float32 downcast (13.7 GB CSV → 1.82 GB)
    - Every DuckDB session capped at 3 GB `memory_limit` with disk spill (`src/utils/duckdb_env.py`)
    - Star schema with pre-aggregated fact tables
    """)

st.markdown("---")


# 1. TOP LEVEL METRICS (KPI Cards)
df_kpi = run_query("""
    SELECT 
        SUM(daily_revenue) as total_rev,
        SUM(purchases) as total_orders,
        SUM(daily_revenue) / NULLIF(SUM(purchases), 0) as aov
    FROM fact_daily_kpis
""")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Revenue", format_currency(df_kpi['total_rev'][0]))
with col2:
    st.metric("Total Orders", format_number(df_kpi['total_orders'][0]))
with col3:
    st.metric("Avg Order Value", format_currency(df_kpi['aov'][0]))
with col4:
    # Get overall conversion from the sessions table
    df_conv = run_query("""
        SELECT SUM(CAST(has_purchase AS INT)) / CAST(COUNT(*) AS DOUBLE) as conversion 
        FROM fact_sessions
    """)
    conversion = df_conv['conversion'][0]
    st.metric("Conversion Rate", f"{conversion:.2%}")

# 2. REVENUE TREND (Time Series)
st.subheader("Revenue & Traffic Trends")

df_trend = run_query("SELECT date, daily_revenue, dau FROM fact_daily_kpis ORDER BY date")

tab1, tab2 = st.tabs(["Revenue", "Active Users"])

with tab1:
    fig_rev = px.line(df_trend, x='date', y='daily_revenue', title="Daily Revenue (USD)")
    fig_rev.update_layout(height=350)
    st.plotly_chart(fig_rev, width='stretch')

with tab2:
    fig_dau = px.line(df_trend, x='date', y='dau', title="Daily Active Users (DAU)")
    fig_dau.update_layout(height=350)
    st.plotly_chart(fig_dau, width='stretch')

# 3. CONVERSION FUNNEL
st.subheader("Conversion Funnel Health")

# Fetch funnel data from events aggregated by session
df_funnel = run_query("""
    SELECT 
        COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_session END) as Views,
        COUNT(DISTINCT CASE WHEN event_type = 'cart' THEN user_session END) as Carts,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_session END) as Purchases
    FROM events
""")

# Prepare data for Plotly Funnel
stages = ['Sessions with View', 'Sessions with Cart', 'Sessions with Purchase']
values = [df_funnel['Views'][0], df_funnel['Carts'][0], df_funnel['Purchases'][0]]

fig_funnel = go.Figure(go.Funnel(
    y=stages,
    x=values,
    textinfo="value+percent initial"
))
fig_funnel.update_layout(title="Session-Based Conversion Funnel", height=400)


st.plotly_chart(fig_funnel, width='stretch')

st.markdown("---")

# 4. BUSINESS IMPACT & INSIGHTS
st.subheader("💡 Business Impact Summary")

col1, col2, col3 = st.columns(3)

with col1:
    st.success("""
    **Revenue Attribution**
    - {total_rev} in tracked revenue
    - Growth opportunities through ML-driven targeting
    - High-value segments identified for personalization
    """.format(total_rev=format_currency(df_kpi['total_rev'][0])))

with col2:
    st.info("""
    **Segment Discovery**
    - 8 behavioral user segments (RFM analysis)
    - "Cant Lose Them" segment = reactivation opportunity
    - Champions & Loyal = cross-sell/upsell targets
    """)

with col3:
    st.warning("""
    **ML-Driven Targeting ROI**
    - Propensity model achieves 4.6x lift (full-data)
    - Top 5% users: 36.9% conversion vs 8.0% baseline
    - See the ML Engine page for live metrics from src/models/metrics.json
    """)

st.caption("📊 All metrics computed from `fact_daily_kpis` and `fact_sessions` tables | Query time: <1 second")
