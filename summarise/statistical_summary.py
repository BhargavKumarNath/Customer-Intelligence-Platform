import polars as pl
import numpy as np
from datetime import datetime
from pathlib import Path

def print_section(title):
    """Print formatted section header"""
    print("\n\n")
    print(f" {title}")

def format_number(num):
    """Format large numbers with commas"""
    return f"{num:,}"

def explain_dataset_structure():
    """Explain the dataset structure before showing statistics"""
    print_section("DATASET STRUCTURE EXPLANATION")
    
    print("""
This analysis uses TWO types of data files for optimal performance:

┌─────────────────────────────────────────────────────────────────────────┐
│ 1. MAIN DATASET: ecommerce_optimized.parquet (1.74 GB)                 │
├─────────────────────────────────────────────────────────────────────────┤
│ • Contains: 109,950,743 individual user events (rows)                  │
│ • Structure: EVENT LOG FORMAT - each row = one action                  │
│ • Example: "User 12345 viewed Product 999 at 2019-10-01 10:00:00"     │
│ • Columns: event_time, event_type, product_id, user_id, price, etc.   │
│ • Use for: Detailed analysis, custom queries, event sequences          │
└─────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────┐
│ 2. PRE-AGGREGATED SUBSETS: analysis_subsets/ (~150 MB total)           │
├─────────────────────────────────────────────────────────────────────────┤
│ ✓ product_summary.parquet (206,876 products)                           │
│   → One row per product with total views, carts, purchases             │
│   → Example: "Product 999: 50K views, 5K carts, 2K purchases"          │
│                                                                         │
│ ✓ user_summary.parquet (5,316,649 users)                               │
│   → One row per user with their total behavior                         │
│   → Example: "User 12345: 47 events, 4 purchases, $1,245 spent"        │
│                                                                         │
│ ✓ daily_summary.parquet (182 days × 3 event types)                     │
│   → One row per day per event type                                     │
│   → Example: "2019-10-01 had 450K views, 45K carts, 12K purchases"     │
└─────────────────────────────────────────────────────────────────────────┘

WHY BOTH?
• Subsets = Fast & convenient (loads instantly, common queries pre-computed)
• Main = Flexible & detailed (can answer any question, event-level data)

WHAT THIS REPORT USES:
• Sections 1-2: Main dataset (event counts, distributions)
• Sections 3-10: Pre-aggregated subsets (user/product/time analysis)
    """)
    
def comprehensive_dataset_summary(
    main_file: str = "ecommerce_optimized.parquet",
    products_file: str = "analysis_subsets/product_summary.parquet",
    users_file: str = "analysis_subsets/user_summary.parquet",
    daily_file: str = "analysis_subsets/daily_summary.parquet"
):
    """
    Generate comprehensive statistical summary of e-commerce dataset
    Uses both full data (lazy) and pre-aggregated subsets for efficiency
    """
    
    # First explain the structure
    explain_dataset_structure()
    
    print_section("E-COMMERCE DATASET STATISTICAL SUMMARY")
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load data
    df = pl.scan_parquet(main_file)
    products = pl.read_parquet(products_file)
    users = pl.read_parquet(users_file)
    daily = pl.read_parquet(daily_file)
    
    # 1. DATASET OVERVIEW
    print_section("1. DATASET OVERVIEW")
    print("Data Source: Main dataset (ecommerce_optimized.parquet)\n")
    
    total_rows = df.select(pl.len()).collect().item()
    schema = df.collect_schema()
    
    print(f"Total Events:        {format_number(total_rows)}")
    print(f"Total Columns:       {len(schema)}")
    print(f"Unique Products:     {format_number(len(products))}")
    print(f"Unique Users:        {format_number(len(users))}")
    print(f"Date Range:          {daily['date'].min()} to {daily['date'].max()}")
    print(f"Days of Data:        {len(daily['date'].unique())}")
    
    # File size
    file_size = Path(main_file).stat().st_size / (1024**3)
    print(f"File Size:           {file_size:.2f} GB")
    
    print("\n💡 Note: Each row in the main dataset = one user action (event)")
    print("   Example: 'User X viewed Product Y at Time Z' is 1 row")
    
    # 2. EVENT TYPE DISTRIBUTION
    print_section("2. EVENT TYPE DISTRIBUTION")
    print("Data Source: Main dataset (ecommerce_optimized.parquet)\n")
    
    event_counts = (
        df.group_by("event_type")
        .agg(pl.len().alias("count"))
        .collect()
        .sort("count", descending=True)
    )
    
    total_events = event_counts["count"].sum()
    
    for row in event_counts.iter_rows(named=True):
        event = row["event_type"]
        count = row["count"]
        pct = (count / total_events) * 100
        print(f"  {event:15s}: {format_number(count):>15s} ({pct:5.2f}%)")
    
    # 3. USER BEHAVIOR STATISTICS
    print_section("3. USER BEHAVIOR STATISTICS")
    print("Data Source: analysis_subsets/user_summary.parquet")
    print("(One row per user with aggregated behavior metrics)\n")
    
    # User engagement metrics
    user_stats = users.select([
        pl.col("total_events").mean().alias("avg_events"),
        pl.col("total_events").median().alias("median_events"),
        pl.col("total_events").max().alias("max_events"),
        pl.col("views").mean().alias("avg_views"),
        pl.col("carts").mean().alias("avg_carts"),
        pl.col("purchases").mean().alias("avg_purchases"),
        pl.col("unique_products").mean().alias("avg_unique_products"),
        pl.col("total_spent").mean().alias("avg_total_spent"),
    ]).to_dicts()[0]
    
    print(f"Average events per user:     {user_stats['avg_events']:.2f}")
    print(f"Median events per user:      {user_stats['median_events']:.0f}")
    print(f"Max events per user:         {format_number(user_stats['max_events'])}")
    print(f"Average views per user:      {user_stats['avg_views']:.2f}")
    print(f"Average carts per user:      {user_stats['avg_carts']:.2f}")
    print(f"Average purchases per user:  {user_stats['avg_purchases']:.2f}")
    print(f"Average unique products:     {user_stats['avg_unique_products']:.2f}")
    print(f"Average total spent:         ${user_stats['avg_total_spent']:.2f}")
    
    # User segmentation
    print("\nUser Segmentation:")
    user_segments = (
        users
        .with_columns([
            pl.when(pl.col("purchases") > 10).then(pl.lit("High Buyer (10+)"))
              .when(pl.col("purchases") > 5).then(pl.lit("Medium Buyer (5-10)"))
              .when(pl.col("purchases") > 0).then(pl.lit("Low Buyer (1-5)"))
              .when(pl.col("carts") > 0).then(pl.lit("Cart User"))
              .otherwise(pl.lit("Browser Only"))
              .alias("segment")
        ])
        .group_by("segment")
        .agg([
            pl.len().alias("count"),
            (pl.len() / len(users) * 100).alias("pct")
        ])
        .sort("count", descending=True)
    )
    
    for row in user_segments.iter_rows(named=True):
        print(f"  {row['segment']:20s}: {format_number(row['count']):>12s} ({row['pct']:5.2f}%)")
    
    # 4. PRODUCT STATISTICS
    print_section("4. PRODUCT STATISTICS")
    print("Data Source: analysis_subsets/product_summary.parquet")
    print("(One row per product with aggregated performance metrics)\n")
    
    product_stats = products.select([
        pl.col("total_events").mean().alias("avg_events"),
        pl.col("views").mean().alias("avg_views"),
        pl.col("carts").mean().alias("avg_carts"),
        pl.col("purchases").mean().alias("avg_purchases"),
        pl.col("unique_users").mean().alias("avg_unique_users"),
        pl.col("price").mean().alias("avg_price"),
        pl.col("price").median().alias("median_price"),
        pl.col("price").min().alias("min_price"),
        pl.col("price").max().alias("max_price"),
    ]).to_dicts()[0]
    
    print(f"Average events per product:    {product_stats['avg_events']:.2f}")
    print(f"Average views per product:     {product_stats['avg_views']:.2f}")
    print(f"Average carts per product:     {product_stats['avg_carts']:.2f}")
    print(f"Average purchases per product: {product_stats['avg_purchases']:.2f}")
    print(f"Average unique users:          {product_stats['avg_unique_users']:.2f}")
    
    print(f"\nPrice Distribution:")
    print(f"  Average:  ${product_stats['avg_price']:.2f}")
    print(f"  Median:   ${product_stats['median_price']:.2f}")
    print(f"  Min:      ${product_stats['min_price']:.2f}")
    print(f"  Max:      ${product_stats['max_price']:.2f}")
    
    # Products with purchases
    products_with_sales = products.filter(pl.col("purchases") > 0)
    print(f"\nProducts with sales: {format_number(len(products_with_sales))} ({len(products_with_sales)/len(products)*100:.1f}%)")
    
    # Top products
    print("\nTop 10 Products by Views:")
    top_viewed = products.sort("views", descending=True).head(10)
    for idx, row in enumerate(top_viewed.iter_rows(named=True), 1):
        print(f"  {idx:2d}. Product {row['product_id']:8d}: {format_number(row['views']):>10s} views")
    
    # 5. CONVERSION FUNNEL
    print_section("5. CONVERSION FUNNEL ANALYSIS")
    print("Data Source: Main dataset (event type counts)\n")
    
    funnel = event_counts.to_dict()
    view_idx = [i for i, e in enumerate(funnel['event_type']) if e == 'view'][0]
    cart_idx = [i for i, e in enumerate(funnel['event_type']) if e == 'cart'][0]
    purchase_idx = [i for i, e in enumerate(funnel['event_type']) if e == 'purchase'][0]
    
    views = funnel['count'][view_idx]
    carts = funnel['count'][cart_idx]
    purchases = funnel['count'][purchase_idx]
    
    view_to_cart = (carts / views) * 100 if views > 0 else 0
    cart_to_purchase = (purchases / carts) * 100 if carts > 0 else 0
    view_to_purchase = (purchases / views) * 100 if views > 0 else 0
    
    print(f"Views:               {format_number(views)}")
    print(f"  ↓ {view_to_cart:.2f}%")
    print(f"Carts:               {format_number(carts)}")
    print(f"  ↓ {cart_to_purchase:.2f}%")
    print(f"Purchases:           {format_number(purchases)}")
    print(f"\nOverall Conversion:  {view_to_purchase:.2f}% (view → purchase)")
    
    # 6. BRAND ANALYSIS
    print_section("6. BRAND ANALYSIS")
    print("Data Source: analysis_subsets/product_summary.parquet")
    print("   (Aggregated by brand from product-level data)\n")
    
    # Count products by brand
    brand_counts = (
        products
        .filter(pl.col("brand").is_not_null())
        .group_by("brand")
        .agg([
            pl.len().alias("product_count"),
            pl.col("purchases").sum().alias("total_purchases"),
            pl.col("views").sum().alias("total_views"),
        ])
        .sort("total_purchases", descending=True)
    )
    
    total_brands = len(brand_counts)
    print(f"Total Brands: {format_number(total_brands)}")
    
    print("\nTop 15 Brands by Purchases:")
    for idx, row in enumerate(brand_counts.head(15).iter_rows(named=True), 1):
        brand = row['brand'] if row['brand'] else 'Unknown'
        purchases = row['total_purchases']
        products_cnt = row['product_count']
        print(f"  {idx:2d}. {brand:25s}: {format_number(purchases):>10s} purchases ({products_cnt:,} products)")
    
    # 7. CATEGORY ANALYSIS
    print_section("7. CATEGORY ANALYSIS")
    print("Data Source: Main dataset (ecommerce_optimized.parquet)")
    print("(Aggregated from event-level data by category)\n")
    
    category_stats = (
        df
        .filter(pl.col("category_code").is_not_null())
        .group_by("category_code")
        .agg([
            pl.len().alias("total_events"),
            pl.col("event_type").filter(pl.col("event_type") == "purchase").len().alias("purchases"),
        ])
        .collect()
        .sort("purchases", descending=True)
    )
    
    total_categories = len(category_stats)
    print(f"Total Categories: {format_number(total_categories)}")
    
    print("\nTop 15 Categories by Purchases:")
    for idx, row in enumerate(category_stats.head(15).iter_rows(named=True), 1):
        cat = row['category_code']
        purchases = row['purchases']
        events = row['total_events']
        print(f"  {idx:2d}. {cat:40s}: {format_number(purchases):>10s} purchases")
    
    # 8. TIME-BASED ANALYSIS
    print_section("8. TIME-BASED PATTERNS")
    print("Data Source: analysis_subsets/daily_summary.parquet")
    print("(One row per day per event type with aggregated metrics)\n")
    
    # Daily aggregates
    daily_agg = (
        daily
        .group_by("date")
        .agg([
            pl.col("event_count").sum().alias("total_events"),
            pl.col("unique_users").sum().alias("total_users"),
            pl.col("revenue").sum().alias("total_revenue"),
        ])
        .sort("date")
    )
    
    print(f"Average events per day:   {format_number(int(daily_agg['total_events'].mean()))}")
    print(f"Average users per day:    {format_number(int(daily_agg['total_users'].mean()))}")
    print(f"Average revenue per day:  ${daily_agg['total_revenue'].mean():.2f}")
    
    print(f"\nBusiest day:   {daily_agg.sort('total_events', descending=True).head(1)['date'][0]} "
          f"({format_number(daily_agg['total_events'].max())} events)")
    print(f"Slowest day:   {daily_agg.sort('total_events').head(1)['date'][0]} "
          f"({format_number(daily_agg['total_events'].min())} events)")
    
    # Revenue statistics
    purchase_daily = daily.filter(pl.col("event_type") == "purchase")
    if len(purchase_daily) > 0:
        print(f"\nTotal Revenue:        ${purchase_daily['revenue'].sum():,.2f}")
        print(f"Peak revenue day:     {purchase_daily.sort('revenue', descending=True).head(1)['date'][0]} "
              f"(${purchase_daily['revenue'].max():,.2f})")
    
    # 9. DATA QUALITY METRICS
    print_section("9. DATA QUALITY METRICS")
    print("Data Source: Main dataset (ecommerce_optimized.parquet)\n")
    
    # Null counts
    null_summary = (
        df.select([
            pl.col("category_code").is_null().sum().alias("category_code_nulls"),
            pl.col("brand").is_null().sum().alias("brand_nulls"),
            pl.col("user_session").is_null().sum().alias("user_session_nulls"),
        ])
        .collect()
        .to_dicts()[0]
    )
    
    print(f"Missing category_code: {format_number(null_summary['category_code_nulls'])} ({null_summary['category_code_nulls']/total_rows*100:.1f}%)")
    print(f"Missing brand:         {format_number(null_summary['brand_nulls'])} ({null_summary['brand_nulls']/total_rows*100:.1f}%)")
    print(f"Missing user_session:  {format_number(null_summary['user_session_nulls'])} ({null_summary['user_session_nulls']/total_rows*100:.1f}%)")
    
    # Session statistics
    session_stats = (
        df
        .filter(pl.col("user_session").is_not_null())
        .group_by("user_session")
        .agg(pl.len().alias("events_per_session"))
        .select([
            pl.col("events_per_session").mean().alias("avg"),
            pl.col("events_per_session").median().alias("median"),
            pl.col("events_per_session").max().alias("max"),
        ])
        .collect()
        .to_dicts()[0]
    )
    
    print(f"\nSession Statistics:")
    print(f"  Average events per session: {session_stats['avg']:.2f}")
    print(f"  Median events per session:  {session_stats['median']:.0f}")
    print(f"  Max events per session:     {format_number(session_stats['max'])}")
    
    # 10. KEY INSIGHTS
    print_section("10. KEY BUSINESS INSIGHTS")
    print("Data Sources: Combined analysis from all datasets\n")
    
    # Calculate key metrics
    active_buyers = users.filter(pl.col("purchases") > 0)
    buyer_rate = len(active_buyers) / len(users) * 100
    
    avg_order_value = (
        users.filter(pl.col("purchases") > 0)
        .select((pl.col("total_spent") / pl.col("purchases")).mean())
        .item()
    )
    
    print(f"✓ Buyer Rate:           {buyer_rate:.2f}% of users made a purchase")
    print(f"✓ Average Order Value:  ${avg_order_value:.2f}")
    print(f"✓ Conversion Rate:      {view_to_purchase:.2f}% (view → purchase)")
    print(f"✓ Cart Abandonment:     {100 - cart_to_purchase:.2f}%")
    print(f"✓ Active Products:      {len(products_with_sales)/len(products)*100:.1f}% of products have sales")
    
    print("\n" + "="*80)
    print("Summary generation complete!")
    print("="*80 + "\n")


if __name__ == "__main__":
    # Run comprehensive summary
    comprehensive_dataset_summary(
        main_file="data/ecommerce_optimized.parquet",
        products_file="analysis_subsets/product_summary.parquet",
        users_file="analysis_subsets/user_summary.parquet",
        daily_file="analysis_subsets/daily_summary.parquet"
    )