import polars as pl
import numpy as np
from pathlib import Path

def optimize_ecommerce_dataset(input_path: str, output_path: str):
    """
    Optimize large e-commerce dataset for 16GB RAM system
    
    Parameters:
    -----------
    input_path : str
        Path to input parquet file
    output_path : str
        Path to save optimized parquet file
    """
    
    print("Starting optimization process...")
    print(f"Reading from: {input_path}")
    
    # Read with streaming to avoid memory issues
    df = pl.scan_parquet(input_path)
    
    # Apply optimizations
    df_optimized = df.select([
        # Parse datetime once and store as optimal type
        pl.col("event_time").str.to_datetime("%Y-%m-%d %H:%M:%S UTC").alias("event_time"),
        
        # Event type - categorical with few unique values
        pl.col("event_type").cast(pl.Categorical),
        
        # Product ID - reduce from Int64 to Int32 (max: 100M fits in Int32)
        pl.col("product_id").cast(pl.Int32),
        
        # Category ID - needs Int64 but we can try dictionary encoding
        pl.col("category_id"),
        
        # Category code - categorical (many nulls, hierarchical structure)
        pl.col("category_code").cast(pl.Categorical),
        
        # Brand - categorical (many nulls)
        pl.col("brand").cast(pl.Categorical),
        
        # Price - reduce precision from Float64 to Float32
        pl.col("price").cast(pl.Float32),
        
        # User ID - reduce from Int64 to Int32
        pl.col("user_id").cast(pl.Int32),
        
        # User session - categorical (UUID)
        pl.col("user_session").cast(pl.Categorical),
    ])
    
    # Collect and save with optimized settings
    print("Collecting and optimizing data...")
    
    df_optimized.sink_parquet(
        output_path,
        compression="zstd",  # Better compression than snappy
        compression_level=3,  # Balanced speed/compression (10 was too slow)
        statistics=True,
        row_group_size=500_000,  # Larger row groups for better query performance
    )
    
    print(f"Optimized file saved to: {output_path}")
    
    # Print comparison statistics
    print("\n" + "="*60)
    print("OPTIMIZATION SUMMARY")
    print("="*60)
    
    original_size = Path(input_path).stat().st_size / (1024**3)
    optimized_size = Path(output_path).stat().st_size / (1024**3)
    
    print(f"Original size: {original_size:.2f} GB")
    print(f"Optimized size: {optimized_size:.2f} GB")
    print(f"Size reduction: {(1 - optimized_size/original_size)*100:.1f}%")
    
    # Memory usage estimate
    print("\n" + "="*60)
    print("ESTIMATED MEMORY USAGE PER COLUMN (110M rows)")
    print("="*60)
    print(f"event_time (Datetime):      ~880 MB")
    print(f"event_type (Categorical):   ~110 MB")
    print(f"product_id (Int32):         ~440 MB")
    print(f"category_id (Int64):        ~880 MB")
    print(f"category_code (Categorical):~200 MB (est)")
    print(f"brand (Categorical):        ~150 MB (est)")
    print(f"price (Float32):            ~440 MB")
    print(f"user_id (Int32):            ~440 MB")
    print(f"user_session (Categorical): ~200 MB (est)")
    print(f"{'─'*60}")
    print(f"TOTAL ESTIMATED:            ~3.7 GB in memory")
    print(f"Your available RAM:         16 GB")
    print(f"Safety margin:              ~12 GB for processing")


def create_analysis_ready_chunks(input_path: str, output_dir: str, chunk_size: int = 10_000_000):
    """
    Create chunked datasets for analysis if full dataset is still too large
    
    Parameters:
    -----------
    input_path : str
        Path to optimized parquet file
    output_dir : str
        Directory to save chunk files
    chunk_size : int
        Rows per chunk (default: 10M rows ~350MB each)
    """
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    print(f"Creating analysis chunks in: {output_dir}")
    
    df = pl.scan_parquet(input_path)
    
    # Get total rows
    total_rows = df.select(pl.len()).collect().item()
    n_chunks = (total_rows + chunk_size - 1) // chunk_size
    
    print(f"Total rows: {total_rows:,}")
    print(f"Chunk size: {chunk_size:,}")
    print(f"Number of chunks: {n_chunks}")
    
    for i in range(n_chunks):
        offset = i * chunk_size
        chunk_file = output_path / f"chunk_{i:03d}.parquet"
        
        print(f"Processing chunk {i+1}/{n_chunks}...")
        
        df.slice(offset, chunk_size).sink_parquet(
            chunk_file,
            compression="zstd",
            compression_level=10,
        )
    
    print(f"\nCreated {n_chunks} chunk files")
    print("Use with: pl.scan_parquet('chunks/*.parquet') for lazy operations")


def load_for_analysis(file_path: str, sample_frac: float = None):
    """
    Load optimized dataset for analysis with optional sampling
    
    Parameters:
    -----------
    file_path : str
        Path to optimized parquet file
    sample_frac : float, optional
        Fraction to sample (e.g., 0.1 for 10%)
    
    Returns:
    --------
    pl.LazyFrame
        Lazy frame ready for analysis
    """
    
    df = pl.scan_parquet(file_path)
    
    if sample_frac:
        print(f"Sampling {sample_frac*100}% of data...")
        df = df.filter(pl.col("user_id") % int(1/sample_frac) == 0)
    
    return df


def create_indexed_subsets(input_path: str, output_dir: str):
    """
    Create pre-aggregated and indexed subsets for common analysis patterns
    This dramatically speeds up repeated queries
    """
    
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    print("Creating analysis-optimized subsets...")
    df = pl.scan_parquet(input_path)
    
    # 1. Product-level aggregations (for product analysis)
    print("Building product summary...")
    product_summary = (
        df.group_by("product_id")
        .agg([
            pl.len().alias("total_events"),
            pl.col("event_type").filter(pl.col("event_type") == "view").len().alias("views"),
            pl.col("event_type").filter(pl.col("event_type") == "cart").len().alias("carts"),
            pl.col("event_type").filter(pl.col("event_type") == "purchase").len().alias("purchases"),
            pl.col("price").first().alias("price"),
            pl.col("brand").first().alias("brand"),
            pl.col("category_code").first().alias("category_code"),
            pl.col("user_id").n_unique().alias("unique_users"),
        ])
        .collect()
    )
    product_summary.write_parquet(output_path / "product_summary.parquet")
    print(f"  → Saved: {len(product_summary):,} products")
    
    # 2. User-level aggregations (for user behavior analysis)
    print("Building user summary...")
    user_summary = (
        df.group_by("user_id")
        .agg([
            pl.len().alias("total_events"),
            pl.col("event_type").filter(pl.col("event_type") == "view").len().alias("views"),
            pl.col("event_type").filter(pl.col("event_type") == "cart").len().alias("carts"),
            pl.col("event_type").filter(pl.col("event_type") == "purchase").len().alias("purchases"),
            pl.col("product_id").n_unique().alias("unique_products"),
            pl.col("price").filter(pl.col("event_type") == "purchase").sum().alias("total_spent"),
        ])
        .collect()
    )
    user_summary.write_parquet(output_path / "user_summary.parquet")
    print(f"  → Saved: {len(user_summary):,} users")
    
    # 3. Daily time-series (for trend analysis)
    print("Building daily time-series...")
    daily_summary = (
        df.with_columns(pl.col("event_time").cast(pl.Date).alias("date"))
        .group_by(["date", "event_type"])
        .agg([
            pl.len().alias("event_count"),
            pl.col("user_id").n_unique().alias("unique_users"),
            pl.col("price").filter(pl.col("event_type") == "purchase").sum().alias("revenue"),
        ])
        .collect()
        .sort("date")
    )
    daily_summary.write_parquet(output_path / "daily_summary.parquet")
    print(f"  → Saved: {len(daily_summary):,} daily records")
    
    print(f"\nAnalysis subsets saved to: {output_dir}")
    print("These are tiny files that load instantly for common analyses!")


if __name__ == "__main__":
    optimize_ecommerce_dataset(
        input_path="data/2019-Oct-Nov.parquet",
        output_path="ecommerce_optimized.parquet"
    )
    
    create_indexed_subsets(
        input_path="ecommerce_optimized.parquet",
        output_dir="analysis_subsets"
    )
        
    print("\n" + "="*60)
    print("LOADING FOR ANALYSIS EXAMPLES")
    print("="*60)
    
    # Full dataset (lazy)
    df_full = pl.scan_parquet("ecommerce_optimized.parquet")
    print("\nFull dataset (lazy):")
    print(df_full.schema)
    
    # Sample 10% for quick exploration
    df_sample = load_for_analysis("ecommerce_optimized.parquet", sample_frac=0.1)
    print("\n10% Sample (lazy):")
    print(df_sample.select(pl.len()).collect())
    
    print("\nExample: Top 10 products by view count")
    top_products = (
        df_full
        .filter(pl.col("event_type") == "view")
        .group_by("product_id")
        .agg(pl.len().alias("view_count"))
        .sort("view_count", descending=True)
        .head(10)
        .collect()
    )
    print(top_products)