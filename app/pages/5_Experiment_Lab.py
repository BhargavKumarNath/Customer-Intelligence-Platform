import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from scipy import stats

st.set_page_config(page_title="Experiment Lab", page_icon="🧪", layout="wide")

st.title("🧪 Causal Inference & Experimentation Lab")
st.markdown("""
**The Problem:** Correlation is not Causation. Just because users who used a coupon spent more, doesn't mean the coupon *caused* it.  
**The Solution:** We use **A/B Testing** (Randomized Controlled Trials) to isolate impact.

👇 **Simulation Sandbox:** Adjust the parameters below to simulate a live experiment on our "Cant Lose Them" segment.
""")

# Methodology Context
with st.expander("🔬 Experiment Design Rationale"):
    st.markdown("""
    **Why "Cant Lose Them" Segment?**
    - High historical value (F≥4) but long recency (R=1)
    - Highest potential ROI from reactivation campaigns
    - Clear success metric: Conversion back to active state
    
    **Statistical Framework:**
    - **Two-tailed T-test:** Detects directional lift in either direction
    - **Confidence Level:** 95% (α=0.05) — standard in industry
    - **Sample Size:** Calculated via power analysis (1-β=0.80 for 15% lift)
    
    **Amazon-Style Bar Raiser:**
    - ✓ Statistical significance (p<0.05)
    - ✓ Business significance (>10% lift threshold)
    - ✓ Scalability (can we operationalize this?)
    
    *Implementation: `src/analysis/ab_testing.py` using SciPy stats library.*
    """)

st.markdown("---")


# 1. EXPERIMENT CONFIGURATION
st.sidebar.header("🔬 Experiment Parameters")

# Sliders
traffic_size = st.sidebar.slider("Sample Size (Users per variant)", 1000, 50000, 18000, step=1000)
baseline_conv = st.sidebar.slider("Baseline Conversion Rate (%)", 1.0, 30.0, 12.0, step=0.5) / 100.0
expected_lift = st.sidebar.slider("Expected Lift (Relative %)", 0.0, 50.0, 15.0, step=1.0) / 100.0
confidence_level = st.sidebar.selectbox("Confidence Level", [0.90, 0.95, 0.99], index=1)

# Run Simulation
st.subheader("Real-Time Simulation Results")

# Generate Synthetic Data based on sliders
np.random.seed(42)
control_conversions = np.random.binomial(n=traffic_size, p=baseline_conv)
treatment_prob = baseline_conv * (1 + expected_lift)
treatment_conversions = np.random.binomial(n=traffic_size, p=treatment_prob)

# Calculate Stats
control_rate = control_conversions / traffic_size
treatment_rate = treatment_conversions / traffic_size
actual_lift = (treatment_rate - control_rate) / control_rate

# Z-Test
se_control = np.sqrt(control_rate * (1 - control_rate) / traffic_size)
se_treatment = np.sqrt(treatment_rate * (1 - treatment_rate) / traffic_size)
se_diff = np.sqrt(se_control**2 + se_treatment**2)
z_score = (treatment_rate - control_rate) / se_diff
p_value = stats.norm.sf(abs(z_score)) * 2  # Two-tailed

# 2. VISUALIZATION
col1, col2 = st.columns([2, 1])

with col1:
    # Distribution Plot
    x = np.linspace(baseline_conv * 0.8, treatment_prob * 1.2, 1000)
    y_control = stats.norm.pdf(x, control_rate, se_control)
    y_treatment = stats.norm.pdf(x, treatment_rate, se_treatment)
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x, y=y_control, name='Control Group', fill='tozeroy', line=dict(color='gray')))
    fig.add_trace(go.Scatter(x=x, y=y_treatment, name='Treatment Group', fill='tozeroy', line=dict(color='green')))
    
    fig.update_layout(
        title="Probability Distributions of Conversion Rates",
        xaxis_title="Conversion Rate",
        yaxis_title="Density",
        height=400
    )
    st.plotly_chart(fig, width='stretch')

with col2:
    st.write("### Statistical Report")
    
    # Metric Cards
    c1, c2 = st.columns(2)
    c1.metric("Control Rate", f"{control_rate:.2%}")
    c2.metric("Treatment Rate", f"{treatment_rate:.2%}")
    st.metric("Observed Lift", f"{actual_lift:.2%}", delta_color="normal")
    
    st.divider()
    
    # Significance Decision
    alpha = 1 - confidence_level
    is_sig = p_value < alpha
    
    if is_sig:
        st.success(f"✅ **Statistically Significant**")
        st.write(f"P-Value: `{p_value:.5f}`")
        st.write(f"We are {confidence_level:.0%} confident this lift is real.")
    else:
        st.error(f"❌ **Not Significant**")
        st.write(f"P-Value: `{p_value:.5f}`")
        st.write("The difference could be due to random noise. Need more sample size.")

# 3. EXPLANATION
st.info("""
**How to interpret this:**
*   **Overlapping Curves:** If the gray and green curves overlap heavily, we cannot distinguish the groups (Not Significant).
*   **Separated Curves:** If they are distinct, we have high confidence in the result.
*   **Try This:** Lower the "Sample Size" to 1,000. Watch how the curves flatten and overlap (higher uncertainty). This demonstrates why **Power Analysis** is critical before running tests.
""")