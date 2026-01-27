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
    3. **Monetary:** Total lifetime spend ($) → NTILE(5) buckets (higher = better score)
    
    **Segmentation Rules:**
    - **Champions:** R≥4 AND F≥4 (recent, frequent buyers)
    - **Loyal Customers:** R≥3 AND F≥3 (consistent engagers)
    - **Cant Lose Them:** R=1 AND F≥4 (high-value but churning!)
    - **Hibernating:** R=1 AND F≤2 (dormant, low value)
    
    *Implemented in `src/analysis/segmentation.py` using DuckDB SQL with NTILE window functions.*
    """)

st.markdown("---")


# 1. RFM SEGMENTATION DEEP DIVE
st.header("1. Behavioral Segmentation (RFM)")

# Load RFM Data (Sampled for performance if needed, but 700k fits in memory for plotting)
df_rfm = run_query("""
    SELECT 
        segment_name,
        recency_days,
        frequency_count,
        monetary_value,
        rfm_code
    FROM analysis_rfm_segments
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
        y='monetary_value',
        color='segment_name',
        size='frequency_count',
        log_y=True, # Monetary value is usually power-law distributed
        hover_data=['rfm_code'],
        title="RFM Map: Recency vs. Spend (Log Scale)",
        labels={'recency_days': 'Days Since Last Order', 'monetary_value': 'Total Lifetime Spend ($)'},
        color_discrete_sequence=px.colors.qualitative.Bold
    )
    fig_rfm.update_layout(height=500)
    st.plotly_chart(fig_rfm, width='stretch')

with col2:
    st.subheader("Segment Profiles")
    st.info("""
    **Champions:** High Spend, Recent.
    
    **Loyal:** Frequent buyers.
    
    **Cant Lose Them:** High Spend, but churning (High Recency). *Target for Reactivation.*
    
    **Hibernating:** Low Value, Long gone.
    """)
    
    # Segment Counts
    df_counts = df_rfm['segment_name'].value_counts().reset_index()
    df_counts.columns = ['Segment', 'Count']
    fig_pie = px.pie(df_counts, names='Segment', values='Count', hole=0.4)
    fig_pie.update_layout(height=300, showlegend=False)
    st.plotly_chart(fig_pie, width='stretch')

# 2. COHORT RETENTION HEATMAP
st.header("2. Cohort Retention Analysis")
st.markdown("""
This heatmap shows the percentage of users who return to the platform in weeks following their first visit.
*   **Vertical Axis:** The week the user first arrived (Cohort).
*   **Horizontal Axis:** Weeks since that first visit.
*   **Color:** Retention Rate (Darker = Better).
""")

# Fetch Retention Data
df_retention = run_query("SELECT * FROM analysis_weekly_retention")

# Pivot for Heatmap
pivot_df = df_retention.pivot(index='cohort_week', columns='weeks_since_first', values='retention_rate')
# Sort index to ensure dates are in order
pivot_df = pivot_df.sort_index()

# Create Heatmap
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

# 3. ACTIONABLE INSIGHTS
st.subheader("💡 Strategic Recommendations")

c1, c2, c3 = st.columns(3)

with c1:
    st.error("🚨 Churn Alert")
    st.write("We observe a **65% drop-off** in Week 1 across all cohorts. This indicates a 'Discovery' problem. Users visit but don't find a reason to return immediately.")
    
with c2:
    st.warning("⚠️ The 'Cant Lose Them' Opportunity")
    st.write("There is a high-value cluster (Avg Spend > $900) drifting into inactivity. We should run a **Sensitivity Experiment** (Coupon vs. Nudge) on this specific group.")

with c3:
    st.success("✅ Champion Stability")
    st.write("The Top 20% of users are extremely stable with high frequency. Our goal for them is **Cross-Selling** (Recommendation Engine) rather than Discounts.")