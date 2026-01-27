import sys
import polars as pl

def main():
    csv_path_one = sys.argv[1]
    csv_path_two = sys.argv[2]
    output_parquet = sys.argv[3]

    lazy_one = pl.scan_csv(csv_path_one)
    lazy_two = pl.scan_csv(csv_path_two)

    combined = pl.concat([lazy_one, lazy_two])

    combined.sink_parquet(output_parquet)

if __name__ == "__main__":
    main()
