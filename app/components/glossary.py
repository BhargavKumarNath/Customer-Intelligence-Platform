import streamlit as st

# Technical glossary for the dashboard
GLOSSARY = {
    "RFM": {
        "full": "Recency, Frequency, Monetary",
        "definition": "Customer segmentation technique based on: How recently they purchased (R), How often they purchase (F), How much they spend (M).",
        "application": "Used to identify Champions, Loyal Customers, At-Risk, and Hibernating segments for targeted interventions."
    },
    "DuckDB": {
        "full": "DuckDB Analytics Database",
        "definition": "Embedded OLAP (Online Analytical Processing) database optimized for analytical queries. Think 'SQLite for analytics'.",
        "application": "Processes 109M events with columnar storage, vectorized execution, and zero infrastructure overhead."
    },
    "Polars": {
        "full": "Polars DataFrame Library",
        "definition": "Lightning-fast DataFrame library written in Rust. Features lazy evaluation and parallel execution.",
        "application": "Used for data preprocessing and optimization - achieved 97% memory reduction through smart type casting."
    },
    "Lift (A/B Testing)": {
        "full": "Relative Lift",
        "definition": "The percentage increase in the treatment group vs control group. Formula: (Treatment Rate - Control Rate) / Control Rate.",
        "application": "A 15% lift means the treatment improved conversion by 15% compared to the baseline."
    },
    "Lift (Market Basket)": {
        "full": "Association Rule Lift",
        "definition": "Strength of association between products. Formula: P(A and B) / (P(A) × P(B)). Lift > 1 means positive correlation.",
        "application": "Lift of 2.5 means customers are 2.5x more likely to buy Product B if they bought Product A."
    },
    "NTILE": {
        "full": "N-Tile Window Function",
        "definition": "SQL function that divides data into N equal-sized buckets based on ordering. Used for quantile-based scoring.",
        "application": "NTILE(5) creates quintiles (1-5 scores) for RFM metrics, enabling rule-based segmentation."
    },
    "LightGBM": {
        "full": "Light Gradient Boosting Machine",
        "definition": "Fast, distributed gradient boosting framework by Microsoft. Uses leaf-wise tree growth for high accuracy.",
        "application": "Trained propensity model with 0.XX AUC-ROC, achieving 4.5x lift vs random targeting."
    },
    "Sessionization": {
        "full": "User Session Construction",
        "definition": "Grouping sequential events into logical sessions (visits) based on UUIDs or time-based windowing.",
        "application": "Converted 109M events into 15M sessions to analyze user journeys and conversion funnels."
    },
    "Dimensional Modeling": {
        "full": "Star Schema Design",
        "definition": "Data warehouse pattern with fact tables (metrics) and dimension tables (attributes) for analytical queries.",
        "application": "Created dim_users, dim_products, fact_sessions for optimized joins and aggregations."
    },
    "Propensity Model": {
        "full": "Purchase Propensity Prediction",
        "definition": "ML model predicting probability of a user making a purchase in the next period based on historical behavior.",
        "application": "Temporal split: October behavior → November purchase prediction. Used for targeted marketing."
    },
    "Cohort Analysis": {
        "full": "Cohort Retention Analysis",
        "definition": "Tracking user groups (cohorts) over time to measure retention, engagement, or other metrics.",
        "application": "Weekly cohorts showing retention rates - identifies when users churn and product-market fit quality."
    },
    "Categorical Encoding": {
        "full": "Dictionary/Category Data Type",
        "definition": "Memory-efficient representation of string columns with many repeated values using integer codes + dictionary.",
        "application": "Reduced UUID columns from ~2GB to ~200MB by storing unique values once and referencing them."
    },
}


def show_glossary(term: str):
    """
    Display a glossary term definition inline.
    
    Parameters:
    -----------
    term : str
        Term to look up in the glossary
    """
    if term in GLOSSARY:
        entry = GLOSSARY[term]
        with st.expander(f"📖 What is {term}?"):
            st.markdown(f"**{entry['full']}**")
            st.write(entry['definition'])
            st.info(f"**In this project:** {entry['application']}")
    else:
        st.warning(f"Term '{term}' not found in glossary.")


def show_full_glossary():
    """
    Display the complete glossary in an organized format.
    """
    st.markdown("## 📚 Technical Glossary")
    st.markdown("*Key terms and concepts used throughout this dashboard*")
    
    # Group by category
    categories = {
        "Data Engineering": ["DuckDB", "Polars", "Dimensional Modeling", "Categorical Encoding", "Sessionization"],
        "Analytics & Segmentation": ["RFM", "NTILE", "Cohort Analysis"],
        "Machine Learning": ["LightGBM", "Propensity Model"],
        "Experimentation": ["Lift (A/B Testing)", "Lift (Market Basket)"]
    }
    
    for category, terms in categories.items():
        st.subheader(category)
        for term in terms:
            if term in GLOSSARY:
                entry = GLOSSARY[term]
                with st.expander(f"**{term}** - {entry['full']}"):
                    st.write(entry['definition'])
                    st.info(f"**In this project:** {entry['application']}")
        st.markdown("---")


def get_term_tooltip(term: str) -> str:
    """
    Get a short tooltip text for a term.
    
    Parameters:
    -----------
    term : str
        Term to look up
        
    Returns:
    --------
    str
        Short definition for tooltip/help text
    """
    if term in GLOSSARY:
        return GLOSSARY[term]['definition']
    return ""
