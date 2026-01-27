import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from db_utils import run_query, format_number, format_currency
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(__file__)), 'components'))
from glossary import show_glossary

st.set_page_config(page_title="Data Explorer", page_icon="🔍", layout="wide")

st.title("🔍 Data Explorer: Understanding the Foundation")

st.markdown("""
Before building models and analytics, we must thoroughly understand the data. This page provides a comprehensive 
exploration of the **109 million events** dataset, including distributions, patterns, and quality insights.
""")

st.markdown("---")

# Section 1: Dataset Overview
st.header("📊 Dataset Overview")

# Get key statistics
try:
    stats_query = """
    SELECT 
        COUNT(*) as total_events,
        COUNT(DISTINCT user_id) as unique_users,
        COUNT(DISTINCT product_id) as unique_products,
        COUNT(DISTINCT user_session) as unique_sessions,
        MIN(event_time) as start_date,
        MAX(event_time) as end_date,
        COUNT(DISTINCT category_code) as unique_categories,
        COUNT(DISTINCT brand) as unique_brands
    FROM events
    """
    stats = run_query(stats_query)
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Events", format_number(stats['total_events'][0]))
    with col2:
        st.metric("Unique Users", format_number(stats['unique_users'][0]))
    with col3:
        st.metric("Unique Products", format_number(stats['unique_products'][0]))
    with col4:
        st.metric("Sessions", format_number(stats['unique_sessions'][0]))
    
    st.markdown(f"""
    **Time Range:** {stats['start_date'][0].strftime('%Y-%m-%d')} to {stats['end_date'][0].strftime('%Y-%m-%d')} 
    ({(stats['end_date'][0] - stats['start_date'][0]).days} days)
    
    **Categories:** {stats['unique_categories'][0]:,} | **Brands:** {stats['unique_brands'][0]:,}
    """)
    
except Exception as e:
    st.error(f"Error loading stats: {e}")

st.markdown("---")

# Section 2: Event Distribution
st.header("📈 Event Type Distribution")

try:
    event_dist_query = """
    SELECT 
        event_type,
        COUNT(*) as event_count,
        COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percentage
    FROM events
    GROUP BY event_type
    ORDER BY event_count DESC
    """
    event_dist = run_query(event_dist_query)
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        fig_pie = px.pie(
            event_dist,
            names='event_type',
            values='event_count',
            title="Event Type Breakdown",
            hole=0.4,
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig_pie.update_traces(textposition='inside', textinfo='label+percent')
        st.plotly_chart(fig_pie, width='stretch')
    
    with col2:
        st.markdown("#### Event Counts")
        for i, row in event_dist.iterrows():
            st.metric(
                row['event_type'].capitalize(),
                format_number(row['event_count']),
                f"{row['percentage']:.1f}%"
            )
        
        # Conversion funnel metrics
        views = event_dist[event_dist['event_type'] == 'view']['event_count'].values[0] if 'view' in event_dist['event_type'].values else 0
        carts = event_dist[event_dist['event_type'] == 'cart']['event_count'].values[0] if 'cart' in event_dist['event_type'].values else 0
        purchases = event_dist[event_dist['event_type'] == 'purchase']['event_count'].values[0] if 'purchase' in event_dist['event_type'].values else 0
        
        if views > 0:
            st.info(f"""
            **Conversion Rates:**
            - View → Cart: {(carts/views*100):.2f}%
            - Cart → Purchase: {(purchases/carts*100 if carts > 0 else 0):.2f}%
            - View → Purchase: {(purchases/views*100):.2f}%
            """)

except Exception as e:
    st.error(f"Error loading event distribution: {e}")

st.markdown("---")

# Section 3: Temporal Patterns
st.header("📅 Temporal Patterns")

try:
    daily_events_query = """
    SELECT 
        CAST(event_time AS DATE) as date,
        COUNT(*) as events,
        COUNT(DISTINCT user_id) as dau,
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as revenue
    FROM events
    GROUP BY 1
    ORDER BY 1
    """
    daily_data = run_query(daily_events_query)
    
    tab1, tab2, tab3 = st.tabs(["Event Volume", "Daily Active Users", "Revenue Trend"])
    
    with tab1:
        fig_events = px.line(
            daily_data,
            x='date',
            y='events',
            title="Daily Event Volume",
            labels={'events': 'Event Count', 'date': 'Date'}
        )
        fig_events.update_layout(height=400)
        st.plotly_chart(fig_events, width='stretch')
        
        st.caption(f"Average: {daily_data['events'].mean():,.0f} events/day | Peak: {daily_data['events'].max():,.0f}")
    
    with tab2:
        fig_dau = px.line(
            daily_data,
            x='date',
            y='dau',
            title="Daily Active Users (DAU)",
            labels={'dau': 'Unique Users', 'date': 'Date'},
            color_discrete_sequence=['#00CC96']
        )
        fig_dau.update_layout(height=400)
        st.plotly_chart(fig_dau, width='stretch')
        
        st.caption(f"Average: {daily_data['dau'].mean():,.0f} users/day | Peak: {daily_data['dau'].max():,.0f}")
    
    with tab3:
        fig_revenue = px.area(
            daily_data,
            x='date',
            y='revenue',
            title="Daily Revenue Trend",
            labels={'revenue': 'Revenue ($)', 'date': 'Date'},
            color_discrete_sequence=['#FF6B6B']
        )
        fig_revenue.update_layout(height=400)
        st.plotly_chart(fig_revenue, width='stretch')
        
        total_revenue = daily_data['revenue'].sum()
        st.caption(f"Total Revenue: {format_currency(total_revenue)} | Average Daily: {format_currency(daily_data['revenue'].mean())}")

except Exception as e:
    st.error(f"Error loading temporal patterns: {e}")

st.markdown("---")

# Section 4: Price Distribution
st.header("💰 Price Distribution Analysis")

try:
    price_stats_query = """
    SELECT 
        MIN(price) as min_price,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY price) as p25,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY price) as median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY price) as p75,
        PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY price) as p90,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY price) as p95,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY price) as p99,
        MAX(price) as max_price,
        AVG(price) as avg_price
    FROM events
    WHERE price > 0
    """
    price_stats = run_query(price_stats_query)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Price Percentiles")
        percentile_df = pd.DataFrame({
            'Percentile': ['Min', '25th', '50th (Median)', '75th', '90th', '95th', '99th', 'Max'],
            'Price ($)': [
                price_stats['min_price'][0],
                price_stats['p25'][0],
                price_stats['median'][0],
                price_stats['p75'][0],
                price_stats['p90'][0],
                price_stats['p95'][0],
                price_stats['p99'][0],
                price_stats['max_price'][0]
            ]
        })
        st.dataframe(percentile_df, width='stretch', hide_index=True)
        
        st.metric("Average Price", format_currency(price_stats['avg_price'][0]))
    
    with col2:
        st.markdown("#### Price Range Distribution (Log Scale)")
        # Create price buckets for visualization
        price_buckets_query = """
        SELECT 
            CASE 
                WHEN price < 10 THEN '$0-10'
                WHEN price < 50 THEN '$10-50'
                WHEN price < 100 THEN '$50-100'
                WHEN price < 500 THEN '$100-500'
                WHEN price < 1000 THEN '$500-1K'
                ELSE '$1K+'
            END as price_range,
            COUNT(*) as count
        FROM events
        WHERE price > 0
        GROUP BY 1
        ORDER BY 
            CASE 
                WHEN price_range = '$0-10' THEN 1
                WHEN price_range = '$10-50' THEN 2
                WHEN price_range = '$50-100' THEN 3
                WHEN price_range = '$100-500' THEN 4
                WHEN price_range = '$500-1K' THEN 5
                ELSE 6
            END
        """
        price_buckets = run_query(price_buckets_query)
        
        fig_price = px.bar(
            price_buckets,
            x='price_range',
            y='count',
            title="Events by Price Range",
            labels={'count': 'Event Count', 'price_range': 'Price Range'},
            log_y=True
        )
        fig_price.update_layout(height=350)
        st.plotly_chart(fig_price, width='stretch')

except Exception as e:
    st.error(f"Error loading price distribution: {e}")

st.markdown("---")

# Section 5: Category & Brand Analysis
st.header("🏷️ Category & Brand Popularity")

try:
    tab1, tab2 = st.tabs(["Top Categories", "Top Brands"])
    
    with tab1:
        category_query = """
        SELECT 
            COALESCE(category_code, 'unknown') as category,
            COUNT(*) as events,
            COUNT(DISTINCT user_id) as users,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases
        FROM events
        GROUP BY 1
        ORDER BY events DESC
        LIMIT 15
        """
        categories = run_query(category_query)
        
        fig_cat = px.bar(
            categories,
            y='category',
            x='events',
            orientation='h',
            title="Top 15 Categories by Event Volume",
            labels={'events': 'Event Count', 'category': 'Category'},
            color='events',
            color_continuous_scale='Blues'
        )
        fig_cat.update_layout(height=500, showlegend=False)
        st.plotly_chart(fig_cat, width='stretch')
    
    with tab2:
        brand_query = """
        SELECT 
            COALESCE(brand, 'unknown') as brand,
            COUNT(*) as events,
            COUNT(DISTINCT user_id) as users,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases
        FROM events
        GROUP BY 1
        ORDER BY events DESC
        LIMIT 15
        """
        brands = run_query(brand_query)
        
        fig_brand = px.bar(
            brands,
            y='brand',
            x='events',
            orientation='h',
            title="Top 15 Brands by Event Volume",
            labels={'events': 'Event Count', 'brand': 'Brand'},
            color='events',
            color_continuous_scale='Greens'
        )
        fig_brand.update_layout(height=500, showlegend=False)
        st.plotly_chart(fig_brand, width='stretch')

except Exception as e:
    st.error(f"Error loading category/brand data: {e}")

st.markdown("---")

# Section 6: User Behavior Patterns
st.header("👤 User Behavior Patterns")

try:
    user_behavior_query = """
    SELECT 
        event_count,
        COUNT(*) as user_count
    FROM dim_users
    GROUP BY event_count
    ORDER BY event_count
    """
    user_behavior = run_query(user_behavior_query)
    
    # Power law check
    st.markdown("""
    **User Activity Distribution (Power Law)**
    
    Most users have minimal interaction (1-10 events), while a small fraction are highly engaged (100+ events).
    This is typical of consumer behavior — the "1% rule" in action.
    """)
    
    fig_power = px.scatter(
        user_behavior,
        x='event_count',
        y='user_count',
        log_x=True,
        log_y=True,
        title="User Activity Distribution (Log-Log Scale)",
        labels={'event_count': 'Events per User', 'user_count': 'Number of Users'},
        color='user_count',
        size='user_count',
        color_continuous_scale='Viridis'
    )
    fig_power.update_layout(height=400, showlegend=False)
    st.plotly_chart(fig_power, width='stretch')
    
    # Summary stats
    user_stats_query = """
    SELECT 
        COUNT(*) as total_users,
        AVG(event_count) as avg_events,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY event_count) as median_events,
        MAX(event_count) as max_events,
        SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END) as buyers,
        SUM(CASE WHEN is_buyer THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as conversion_rate
    FROM dim_users
    """
    user_stats = run_query(user_stats_query)
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Users", format_number(user_stats['total_users'][0]))
    col2.metric("Avg Events/User", f"{user_stats['avg_events'][0]:.1f}")
    col3.metric("Buyers", format_number(user_stats['buyers'][0]))
    col4.metric("Conversion Rate", f"{user_stats['conversion_rate'][0]:.2f}%")

except Exception as e:
    st.error(f"Error loading user behavior: {e}")

st.markdown("---")

# Section 7: Data Quality Insights
st.header("🔍 Data Quality & Completeness")

try:
    null_analysis_query = """
    SELECT 
        COUNT(*) as total_rows,
        SUM(CASE WHEN category_code IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as category_null_pct,
        SUM(CASE WHEN brand IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as brand_null_pct,
        SUM(CASE WHEN price = 0 OR price IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as price_zero_pct
    FROM events
    """
    null_stats = run_query(null_analysis_query)
    
    st.markdown("#### Null/Missing Value Analysis")
    
    null_df = pd.DataFrame({
        'Column': ['category_code', 'brand', 'price (zero or null)'],
        'Missing %': [
            null_stats['category_null_pct'][0],
            null_stats['brand_null_pct'][0],
            null_stats['price_zero_pct'][0]
        ]
    })
    
    fig_null = px.bar(
        null_df,
        x='Column',
        y='Missing %',
        title="Data Completeness by Column",
        labels={'Missing %': 'Percentage Missing (%)'},
        text_auto='.1f',
        color='Missing %',
        color_continuous_scale='Reds'
    )
    fig_null.update_layout(height=350, showlegend=False)
    st.plotly_chart(fig_null, width='stretch')
    
    st.info("""
    **Data Quality Notes:**
    - **category_code:** ~35% null — products may lack categorization (data entry issue)
    - **brand:** ~42% null — generic/unbranded products or missing metadata
    - **price:** Zero prices are rare (<1%) — mostly complete
    
    **Handling Strategy:**
    - COALESCE to 'unknown' for categorical analysis
    - Exclude or flag nulls in precision-critical analyses (e.g., price distribution)
    """)

except Exception as e:
    st.error(f"Error loading data quality metrics: {e}")

st.markdown("---")

# Section 8: Session Characteristics
st.header("⏱️ Session Characteristics")

try:
    session_stats_query = """
    SELECT 
        AVG(duration_sec) as avg_duration,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY duration_sec) as median_duration,
        AVG(event_count) as avg_events_per_session,
        SUM(CASE WHEN has_purchase THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as purchase_session_rate
    FROM fact_sessions
    """
    session_stats = run_query(session_stats_query)
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Avg Duration", f"{session_stats['avg_duration'][0]:.0f}s")
    col2.metric("Median Duration", f"{session_stats['median_duration'][0]:.0f}s")
    col3.metric("Avg Events/Session", f"{session_stats['avg_events_per_session'][0]:.1f}")
    col4.metric("Sessions with Purchase", f"{session_stats['purchase_session_rate'][0]:.2f}%")
    
    st.caption("Session = continuous user activity grouped by UUID. Average ~5 minutes, 6 events per session.")

except Exception as e:
    st.error(f"Error loading session stats: {e}")

st.markdown("---")

# Footer
st.success("""
### 🎯 Key Insights from Data Exploration

1. **Scale:** 109M events across 61 days from 3M users interacting with 100K products
2. **Event Mix:** Heavily view-dominated (~68%), with ~8% conversion rate (view → purchase)
3. **Price Range:** Wide distribution ($1-$5000+), median ~$50, with long tail of premium products
4. **User Behavior:** Power law distribution — most users browse lightly, top 1% are super-engaged
5. **Data Quality:** Good overall completeness; category/brand have ~35-42% nulls (handled gracefully)
6. **Temporal Pattern:** Consistent daily activity with some weekly seasonality

**This foundation enables all downstream analytics, segmentation, and ML modeling.**
""")
