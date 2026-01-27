import streamlit as st
import plotly.express as px
import pandas as pd
from db_utils import run_query

st.set_page_config(page_title="ML Engine", page_icon="🤖", layout="wide")

st.title("🤖 AI & Machine Learning Engine")
st.markdown("""
This module demonstrates two production-grade ML systems operationalized in this platform:
1.  **Item-to-Item Collaborative Filtering** (Market Basket Analysis) for personalization.
2.  **Purchase Propensity Modeling** (LightGBM) for targeted marketing.
""")

# ML Methodology Context
with st.expander("🧠 Model Training & Feature Engineering"):
    st.markdown("""
    **Propensity Model (LightGBM):**
    - **Objective:** Binary classification — will user purchase in November?
    - **Training Data:** Temporal split (October behavior → November target)
    - **Features:** Recency, session count, cart events, active span, product views
    - **Hyperparameters:** num_leaves=31, learning_rate=0.05, early_stopping=50 rounds
    - **Performance:** AUC-ROC ~0.XX, achieving 4.5x lift vs random targeting
    
    **Recommendation Engine (Market Basket):**
    - **Algorithm:** Association rules mining (Lift metric)
    - **Self-Join Strategy:** Product pairs from same sessions (15M sessions)
    - **Filtering:** Min support ≥5 co-occurrences, Lift >1.2
    - **Scale:** Computed 10M+ product pairs in 90 seconds
    
    *Implementations: `src/models/train_propensity.py` and `src/models/recommendations.py`*
    """)

st.markdown("---")


# 1. RECOMMENDATION ENGINE (INTERACTIVE)
st.header("1. Product Recommendation Engine")
st.markdown("""
**Logic:** "People who bought Product A also bought Product B."
**Metric:** **Lift**. A Lift of `2.0` means a user is **2x more likely** to buy Product B if they bought Product A, compared to a random user.
""")

col1, col2 = st.columns([1, 2])

with col1:
    st.subheader("🛍️ Simulator")
    
    # 1. Get Brands that have rules
    try:
        # join with dim_products to get brand names for the IDs in our rules table
        brands_query = """
        SELECT DISTINCT p.brand 
        FROM predictions_product_affinity r
        JOIN dim_products p ON r.product_a = p.product_id
        WHERE p.brand != 'unknown'
        ORDER BY 1
        """
        brands = run_query(brands_query)['brand'].tolist()
        
        selected_brand = st.selectbox("Select a Brand:", brands, index=brands.index('samsung') if 'samsung' in brands else 0)
        
        # 2. Get Products for that brand (that have rules)
        products_query = f"""
        SELECT DISTINCT p.category_code, p.product_id 
        FROM predictions_product_affinity r
        JOIN dim_products p ON r.product_a = p.product_id
        WHERE p.brand = '{selected_brand}'
        LIMIT 50
        """
        df_prods = run_query(products_query)
        
        # Create a friendly display string
        product_options = {f"{row['category_code']} (ID: {row['product_id']})": row['product_id'] for i, row in df_prods.iterrows()}
        
        selected_product_name = st.selectbox("Select a Product:", list(product_options.keys()))
        selected_product_id = product_options[selected_product_name]
        
    except Exception as e:
        st.error(f"Database not ready or empty. Error: {e}")
        st.stop()

with col2:
    st.subheader("🔮 AI Recommendations")
    
    # 3. Get Recommendations
    rec_query = f"""
    SELECT 
        p.brand,
        p.category_code,
        p.current_price,
        r.confidence,
        r.lift,
        r.pair_count
    FROM predictions_product_affinity r
    JOIN dim_products p ON r.product_b = p.product_id
    WHERE r.product_a = {selected_product_id}
    ORDER BY r.lift DESC
    LIMIT 6
    """
    
    recs = run_query(rec_query)
    
    if recs.empty:
        st.warning("No strong associations found for this specific product yet. Try a more popular item.")
    else:
        # Display as nice cards with better formatting to show full values
        for i, row in recs.iterrows():
            with st.container():
                col_cat, col_price, col_lift, col_conf = st.columns([2.5, 1.5, 1.5, 1.5])
                
                with col_cat:
                    st.markdown(f"**{row['category_code']}**")
                    st.caption(f"Brand: {row['brand']}")
                
                with col_price:
                    st.markdown("**Price**")
                    st.markdown(f"<h3 style='margin:0;'>${row['current_price']:.2f}</h3>", unsafe_allow_html=True)
                
                with col_lift:
                    st.markdown("**Lift**")
                    st.markdown(f"<h3 style='margin:0;'>{row['lift']:.2f}x</h3>", unsafe_allow_html=True)
                
                with col_conf:
                    st.markdown("**Confidence**")
                    st.markdown(f"<h3 style='margin:0;'>{row['confidence']*100:.1f}%</h3>", unsafe_allow_html=True)
                
                st.divider()


# 2. PROPENSITY MODEL INSIGHTS
st.header("2. Propensity Model (LightGBM) Insights")
st.markdown("""
We trained a Gradient Boosting model to predict which users will buy in **November** based on their **October** behavior.
""")

c1, c2 = st.columns(2)

with c1:
    st.subheader("Feature Importance")
    st.markdown("What behaviors signal a future purchase?")
    
    # Hardcoded from training logs (Step 6) for visualisation
    data = {
        'Feature': ['Oct Events (Total Activity)', 'Active Span Days', 'Cart Additions', 'Product Views', 'Recency (Days ago)'],
        'Importance (Gain)': [631380, 436977, 411642, 222948, 155719]
    }
    df_imp = pd.DataFrame(data).sort_values('Importance (Gain)', ascending=True)
    
    fig_imp = px.bar(
        df_imp, 
        x='Importance (Gain)', 
        y='Feature', 
        orientation='h',
        title="Top Predictive Drivers",
        color='Importance (Gain)',
        color_continuous_scale='Blues'
    )
    st.plotly_chart(fig_imp, width='stretch')

with c2:
    st.subheader("Model Business Impact")
    st.markdown("How much better is AI than random targeting?")
    
    # Data from our Model Evaluation log
    metrics = {
        'Audience': ['Random Targeting', 'AI Top 5% Segment'],
        'Conversion Rate': [0.0803, 0.3662]
    }
    df_lift = pd.DataFrame(metrics)
    
    fig_lift = px.bar(
        df_lift,
        x='Audience',
        y='Conversion Rate',
        color='Audience',
        title="Conversion Rate Comparison (4.5x Lift)",
        text_auto='.1%',
        color_discrete_sequence=['gray', '#00CC96']
    )
    fig_lift.update_layout(showlegend=False)
    st.plotly_chart(fig_lift, width='stretch')
    
    st.success("""
    **Conclusion:** By targeting the users identified by the LightGBM model, 
    marketing efficiency improves by **450%**.
    """)