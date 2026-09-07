"""Build the memory-optimised Parquet the rest of the pipeline reads.

Input : the raw Kaggle CSVs (``data/2019-Oct.csv``, ``data/2019-Nov.csv`` -
        ~14 GB, 109.95M rows total).
Output: ``data/raw/ecommerce_optimized.parquet`` - the single file
        ``config/config.yaml -> paths.raw_data`` points at and
        ``src/ingestion/loader.py`` ingests into DuckDB.

Why DuckDB and not Polars here
-----------------------------
The earlier version of this module did two Polars passes: ``pl.concat`` of two
``scan_csv`` LazyFrames streamed to one Parquet, then a second pass that
``.cast(pl.Categorical)`` every string column (including ``user_session``) and
streamed again. On a 16 GB box that was already tight; on a smaller machine it
OOMs, because casting the 23M distinct ``user_session`` UUIDs to ``Categorical``
materialises a 23M-entry string dictionary in memory - the opposite of an
optimisation for a near-unique column.

DuckDB reads the CSVs in bounded-memory streaming chunks, spills to
``temp_directory`` when a step needs more than ``memory_limit``, and writes
Parquet with per-row-group dictionary + ZSTD encoding. Low-cardinality columns
(``event_type``, ``brand``, ``category_code``) get dictionary-encoded
automatically; ``user_session`` is left as a plain string (correct for a
near-unique column). The numeric downcasts (``BIGINT -> INTEGER`` for the id
columns, ``DOUBLE -> FLOAT`` for price) are applied in the projection.

Run::

    python summarise/optimize_dataset.py
"""

from __future__ import annotations

from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_CSV_DIR = PROJECT_ROOT / "data"
DEFAULT_INPUT_CSVS = [RAW_CSV_DIR / "2019-Oct.csv", RAW_CSV_DIR / "2019-Nov.csv"]
DEFAULT_OUTPUT = PROJECT_ROOT / "data" / "raw" / "ecommerce_optimized.parquet"

# Bounded so the job fits a small (<= 16 GB) machine. DuckDB spills anything
# larger to `temp_directory`; disk is the cheap resource here, RAM is not.
MEMORY_LIMIT = "5GB"
THREADS = 4
ROW_GROUP_SIZE = 500_000

# Raw event_time looks like "2019-10-01 00:00:00 UTC" - a fixed +00:00 offset,
# so a naive TIMESTAMP (no tz) is exact and half the width of TIMESTAMPTZ.
_TS_FORMAT = "%Y-%m-%d %H:%M:%S UTC"

_OPTIMISED_PROJECTION = """
    strptime(event_time, '{ts_format}')            AS event_time,
    event_type,
    CAST(product_id  AS INTEGER)                   AS product_id,
    category_id,                                   -- kept BIGINT: real ids exceed INT32
    category_code,
    brand,
    CAST(price AS FLOAT)                           AS price,
    CAST(user_id AS INTEGER)                       AS user_id,
    user_session
"""


def _connect(temp_dir: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    con.execute(f"SET threads TO {THREADS}")
    con.execute("SET preserve_insertion_order=false")
    temp_dir.mkdir(parents=True, exist_ok=True)
    con.execute(f"SET temp_directory='{temp_dir.as_posix()}'")
    return con


def _csv_relation_sql(csv_paths: list[Path]) -> str:
    files = ", ".join(f"'{p.as_posix()}'" for p in csv_paths)
    # Explicit column types: never let the sniffer widen an id column or read
    # price as DECIMAL. names/dtypes match the raw Kaggle header exactly.
    return f"""
        read_csv(
            [{files}],
            header = true,
            columns = {{
                'event_time': 'VARCHAR',
                'event_type': 'VARCHAR',
                'product_id': 'BIGINT',
                'category_id': 'BIGINT',
                'category_code': 'VARCHAR',
                'brand': 'VARCHAR',
                'price': 'DOUBLE',
                'user_id': 'BIGINT',
                'user_session': 'VARCHAR'
            }}
        )
    """


def optimize_ecommerce_dataset(
    input_csvs: list[Path] | None = None, output_path: Path | None = None
) -> Path:
    """Stream the raw CSVs into one type-optimised, ZSTD Parquet file."""
    input_csvs = input_csvs or DEFAULT_INPUT_CSVS
    output_path = output_path or DEFAULT_OUTPUT
    missing = [p for p in input_csvs if not p.exists()]
    if missing:
        raise FileNotFoundError(
            "Raw CSV(s) not found: " + ", ".join(str(p) for p in missing)
        )
    output_path.parent.mkdir(parents=True, exist_ok=True)

    print("Starting optimisation process...")
    print(f"Reading from : {[str(p) for p in input_csvs]}")
    print(f"Writing to   : {output_path}")

    con = _connect(output_path.parent / ".duckdb_tmp")
    projection = _OPTIMISED_PROJECTION.format(ts_format=_TS_FORMAT)
    relation = _csv_relation_sql(input_csvs)
    con.execute(
        f"""
        COPY (SELECT {projection} FROM {relation})
        TO '{output_path.as_posix()}'
        (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 3,
         ROW_GROUP_SIZE {ROW_GROUP_SIZE})
        """
    )

    raw_bytes = sum(p.stat().st_size for p in input_csvs)
    opt_bytes = output_path.stat().st_size
    rows = con.execute(
        f"SELECT COUNT(*) FROM read_parquet('{output_path.as_posix()}')"
    ).fetchone()[0]
    con.close()

    print("\n" + "=" * 60)
    print("OPTIMISATION SUMMARY")
    print("=" * 60)
    print(f"Rows written    : {rows:,}")
    print(f"Raw CSV size    : {raw_bytes / 1024**3:.2f} GB")
    print(f"Optimised size  : {opt_bytes / 1024**3:.2f} GB")
    print(f"Disk reduction  : {(1 - opt_bytes / raw_bytes) * 100:.1f}%")
    return output_path


def create_indexed_subsets(parquet_path: Path | None = None, output_dir: Path | None = None) -> None:
    """Pre-aggregated helper tables for quick, memory-cheap exploratory work.

    Tiny outputs (one row per product / per user / per day); each is a single
    streaming aggregation over the optimised Parquet, run in DuckDB so it stays
    within ``MEMORY_LIMIT`` regardless of how large the source grows.
    """
    parquet_path = parquet_path or DEFAULT_OUTPUT
    output_dir = output_dir or (PROJECT_ROOT / "data" / "analysis_subsets")
    output_dir.mkdir(parents=True, exist_ok=True)
    con = _connect(output_dir / ".duckdb_tmp")
    src = f"read_parquet('{parquet_path.as_posix()}')"

    print("Building product_summary.parquet ...")
    con.execute(
        f"""
        COPY (
            SELECT product_id,
                   COUNT(*)                                              AS total_events,
                   COUNT(*) FILTER (WHERE event_type = 'view')           AS views,
                   COUNT(*) FILTER (WHERE event_type = 'cart')           AS carts,
                   COUNT(*) FILTER (WHERE event_type = 'purchase')       AS purchases,
                   any_value(price)                                      AS price,
                   any_value(brand)                                     AS brand,
                   any_value(category_code)                             AS category_code,
                   COUNT(DISTINCT user_id)                              AS unique_users
            FROM {src} GROUP BY product_id
        ) TO '{(output_dir / "product_summary.parquet").as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    print("Building user_summary.parquet ...")
    con.execute(
        f"""
        COPY (
            SELECT user_id,
                   COUNT(*)                                                       AS total_events,
                   COUNT(*) FILTER (WHERE event_type = 'view')                    AS views,
                   COUNT(*) FILTER (WHERE event_type = 'cart')                    AS carts,
                   COUNT(*) FILTER (WHERE event_type = 'purchase')                AS purchases,
                   COUNT(DISTINCT product_id)                                     AS unique_products,
                   SUM(price) FILTER (WHERE event_type = 'purchase')              AS total_spent
            FROM {src} GROUP BY user_id
        ) TO '{(output_dir / "user_summary.parquet").as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )

    print("Building daily_summary.parquet ...")
    con.execute(
        f"""
        COPY (
            SELECT CAST(event_time AS DATE)                                       AS date,
                   event_type,
                   COUNT(*)                                                       AS event_count,
                   COUNT(DISTINCT user_id)                                        AS unique_users,
                   SUM(price) FILTER (WHERE event_type = 'purchase')              AS revenue
            FROM {src} GROUP BY 1, 2 ORDER BY 1, 2
        ) TO '{(output_dir / "daily_summary.parquet").as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)
        """
    )
    con.close()
    print(f"Analysis subsets written to: {output_dir}")


if __name__ == "__main__":
    out = optimize_ecommerce_dataset()
    create_indexed_subsets(out)
