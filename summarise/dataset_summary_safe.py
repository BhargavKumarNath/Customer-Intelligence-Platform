import polars as pl
import sys

def main():
    parquet_path = sys.argv[1]

    lf = pl.scan_parquet(parquet_path)

    schema = lf.collect_schema()
    row_count = lf.select(pl.len()).collect().item()

    print("\nDATASET OVERVIEW\n")
    print("Rows:", row_count)
    print("Columns:", len(schema))

    print("\nCOLUMN TYPES\n")
    for col, dtype in schema.items():
        print(col, ":", dtype)

    print("\nNULL COUNTS\n")
    for col in schema:
        nulls = lf.select(pl.col(col).null_count()).collect().item()
        print(col, ":", nulls)

    print("\nNUMERIC STATS\n")
    for col, dtype in schema.items():
        if dtype.is_numeric():
            stats = lf.select(
                pl.col(col).min().alias("min"),
                pl.col(col).max().alias("max"),
                pl.col(col).mean().alias("mean")
            ).collect()

            print(col)
            print(stats)

if __name__ == "__main__":
    main()
