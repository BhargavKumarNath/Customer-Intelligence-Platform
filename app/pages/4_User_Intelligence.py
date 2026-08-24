import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from db_utils import run_query, format_currency

st.set_page_config(page_title="User Intelligence", page_icon="👥", layout="wide")

st.title("👥 User Intelligence & Segmentation")
st.markdown("""
**Objective:** Move beyond "average" metrics. We use **RFM Analysis** (Recency, Frequency, Monetary) to classify users into behavioral clusters, 
and **Cohort Analysis** to understand long-term product fit.
""")

# Methodology Explainer
with st.expander("📚 RFM Methodology Explained"):
    st.markdown("""
    **Scoring Algorithm:**
    1. **Recency:** Days since last purchase → NTILE(5) buckets → **Inverted** (shorter = better score)
    2. **Frequency:** Total purchase count → NTILE(5) buckets (higher = better score)
    3. **Monetary:** Total lifetime spend ($) → NTILE(5) buckets (higher = better score, tracked but not
       part of the segment-naming rule below. Segment name is driven by engagement (R, F) only; spend
       is reported per-segment separately)

    **Segmentation Rules** (in evaluation order, first match wins):
    - **Champions:** R≥4 AND F≥4 (recent, frequent buyers)
    - **Loyal Customers:** R≥3 AND F≥3 (consistent engagers)
    - **Promising:** R≥4 AND F≤2 (recent but not yet frequent)
    - **At Risk:** R≤2 AND F≥3 (used to be frequent buyers, haven't been back recently)
    - **Lost:** R≤2 AND F≤2 (low engagement, long gone)
    - **Regular:** everything else

    *Implemented in `src/processing/dimensional_model.py` (shared by the cloud/sample build and the
    Streamlit app) using DuckDB SQL with NTILE window functions.*
    """)

st.markdown("---")


# 1. RFM SEGMENTATION DEEP DIVE
st.header("1. Behavioral Segmentation (RFM)")


# Load RFM Data (Sampled for performance if needed, but 700k fits in memory for plotting)
df_rfm = run_query("""
    SELECT 
        segment,
        recency_days,
        frequency,
        monetary,
        CAST(r_score AS VARCHAR) || CAST(f_score AS VARCHAR) || CAST(m_score AS VARCHAR) as rfm_code
    FROM user_rfm_segments
    -- Sample down for smoother scatter plots if > 50k points
    USING SAMPLE 10000 
""")

col1, col2 = st.columns([3, 1])

with col1:
    st.subheader("Segment Landscape")
    # Interactive Scatter Plot
    fig_rfm = px.scatter(
        df_rfm,
        x='recency_days',
        y='monetary',
        color='segment',
        size='frequency',
        log_y=True, # Monetary value is usually power-law distributed
        hover_data=['rfm_code'],
        title="RFM Map: Recency vs. Spend (Log Scale)",
        labels={'recency_days': 'Days Since Last Order', 'monetary': 'Total Lifetime Spend ($)'},
        color_discrete_sequence=px.colors.qualitative.Bold
    )
    fig_rfm.update_layout(height=500)
    st.plotly_chart(fig_rfm, width='stretch')

with col2:
    st.subheader("Segment Profiles")
    st.info("""
    **Champions:** Recent AND frequent buyers, highest spend.

    **Loyal Customers:** Consistent, if not top-tier, engagers.

    **At Risk:** Used to buy frequently, haven't been back recently. *Target for reactivation.*

    **Lost:** Low engagement, long gone.
    """)
    
    
    # Segment Counts
    df_counts = df_rfm['segment'].value_counts().reset_index()
    df_counts.columns = ['Segment', 'Count']
    fig_pie = px.pie(df_counts, names='Segment', values='Count', hole=0.4)
    fig_pie.update_layout(height=300, showlegend=False)
    st.plotly_chart(fig_pie, width='stretch')

# 2. COHORT RETENTION ANALYSIS
st.header("2. Cohort Retention Analysis")
st.markdown("""
This heatmap shows the percentage of users who return to the platform in weeks following their first visit.
*   **Vertical Axis:** The week the user first arrived (Cohort).
*   **Horizontal Axis:** Weeks since that first visit.
*   **Color:** Retention Rate (Darker = Better).
""")

# Fetch Retention Data (weekly_retention is built by the same shared
# src/processing/dimensional_model.py used for the rest of this cloud/sample
# database, so it no longer needs the full local pipeline to exist).
df_retention = run_query("SELECT * FROM weekly_retention")

# Pivot for Heatmap
pivot_df = df_retention.pivot(index='cohort_week', columns='weeks_since_first', values='retention_rate')
pivot_df = pivot_df.sort_index()

fig_heat = go.Figure(data=go.Heatmap(
    z=pivot_df.values,
    x=pivot_df.columns,
    y=pivot_df.index,
    colorscale='Viridis',
    text=[[f"{val:.1%}" if pd.notnull(val) else "" for val in row] for row in pivot_df.values],
    texttemplate="%{text}",
    showscale=True
))

fig_heat.update_layout(
    title="Weekly User Retention Rates",
    xaxis_title="Weeks Since First Visit",
    yaxis_title="Acquisition Cohort",
    height=600
)

st.plotly_chart(fig_heat, width='stretch')

# 3. INSIGHTS (computed live from the tables above, not hardcoded copy)
st.subheader("💡 Strategic Recommendations")

# Cohort-weighted week-0 -> week-1 retention across all cohorts that have a week-1 yet
week0 = df_retention[df_retention.weeks_since_first == 0][['cohort_week', 'cohort_size']]
week1 = df_retention[df_retention.weeks_since_first == 1][['cohort_week', 'active_users']]
w1_merged = week0.merge(week1, on='cohort_week', how='inner')
week1_retention = w1_merged['active_users'].sum() / w1_merged['cohort_size'].sum() if len(w1_merged) else float('nan')

seg_summary = run_query("""
    SELECT segment, COUNT(*) as n, AVG(monetary) as avg_spend, AVG(recency_days) as avg_recency
    FROM user_rfm_segments GROUP BY segment
""").set_index('segment')

c1, c2, c3 = st.columns(3)

with c1:
    st.error("🚨 Churn Alert")
    st.write(f"We observe a **{1 - week1_retention:.0%} drop-off** by Week 1 across all cohorts. "
             "This indicates a 'Discovery' problem. Users visit but don't find a reason to return immediately.")

with c2:
    if 'At Risk' in seg_summary.index:
        at_risk = seg_summary.loc['At Risk']
        st.warning("⚠️ The 'At Risk' Opportunity")
        st.write(f"There are **{int(at_risk['n']):,} users** (avg spend ${at_risk['avg_spend']:,.0f}, "
                 f"avg {at_risk['avg_recency']:.0f} days since last purchase) who used to buy frequently and have "
                 "drifted into inactivity. We should run a **reactivation experiment** (coupon vs. nudge) on this segment.")
    else:
        st.warning("⚠️ At-Risk Opportunity")
        st.write("No 'At Risk' users in the current segment table.")

with c3:
    if 'Champions' in seg_summary.index:
        champions = seg_summary.loc['Champions']
        st.success("✅ Champion Stability")
        st.write(f"**{int(champions['n']):,} Champions** (avg spend ${champions['avg_spend']:,.0f}) are the highest-value, "
                 "most recently active segment. Our goal for them is **cross-selling** (recommendation engine) rather than discounts.")
    else:
        st.success("✅ Champion Stability")
        st.write("No 'Champions' users in the current segment table.")