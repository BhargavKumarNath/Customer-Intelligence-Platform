"""
Shared dimensional-model builder.

Both `scripts/create_cloud_database.py` (offline cloud-DB build) and
`app/db_utils.py` (Streamlit cloud/sample-mode, built in-process from the
same parquet) used to carry independent, hand-copied SQL for dim_products,
dim_users, fact_sessions, fact_daily_kpis, user_rfm_segments, and
predictions_product_affinity. That duplication let the two implementations
drift. Most notably, both computed RFM recency against `CURRENT_DATE`
instead of the dataset's own reference date, so `recency_days` grew by one
every day regardless of when the underlying 2019 data was captured.

This module is now the single source of truth for that SQL. Callers must
have already created an `events` table on the given connection.
"""

import logging

import duckdb


def _count(con: duckdb.DuckDBPyConnection, table: str) -> int:
    """Return ``SELECT COUNT(*) FROM <table>`` as a plain int.

    DuckDB's typed ``fetchone()`` is ``tuple[Any, ...] | None``; a bare
    ``.fetchone()[0]`` trips mypy --strict on both the ``None`` case and the
    ``Any`` return. COUNT(*) always yields exactly one row, so ``None`` here
    would mean the query itself failed.
    """
    row = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
    if row is None:  # pragma: no cover - COUNT(*) cannot return zero rows
        raise RuntimeError(f"COUNT(*) returned no row for table {table!r}")
    return int(row[0])


def build_dim_products(con: duckdb.DuckDBPyConnection) -> int:
    con.execute("""
        CREATE OR REPLACE TABLE dim_products AS
        SELECT DISTINCT ON (product_id)
            product_id,
            category_id,
            COALESCE(category_code, 'unknown') as category_code,
            COALESCE(brand, 'unknown') as brand,
            price as current_price
        FROM events
        WHERE product_id IS NOT NULL
        ORDER BY product_id, event_time DESC
    """)
    return _count(con, "dim_products")


def build_dim_users(con: duckdb.DuckDBPyConnection) -> int:
    con.execute("""
        CREATE OR REPLACE TABLE dim_users AS
        SELECT
            user_id,
            MIN(event_time) as first_seen,
            MAX(event_time) as last_seen,
            COUNT(*) as event_count,
            COUNT(DISTINCT user_session) as session_count,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchase_count,
            SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as total_spend,
            MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as is_buyer
        FROM events
        GROUP BY user_id
    """)
    return _count(con, "dim_users")


def build_fact_sessions(con: duckdb.DuckDBPyConnection) -> int:
    con.execute("""
        CREATE OR REPLACE TABLE fact_sessions AS
        SELECT
            user_session,
            user_id,
            MIN(event_time) as session_start,
            MAX(event_time) as session_end,
            CAST(EXTRACT(EPOCH FROM (MAX(event_time) - MIN(event_time))) AS INTEGER) as duration_sec,
            COUNT(*) as event_count,
            COUNT(DISTINCT product_id) as unique_products,
            MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as has_purchase,
            SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as session_revenue
        FROM events
        GROUP BY user_session, user_id
    """)
    return _count(con, "fact_sessions")


def build_fact_daily_kpis(con: duckdb.DuckDBPyConnection) -> int:
    con.execute("""
        CREATE OR REPLACE TABLE fact_daily_kpis AS
        SELECT
            CAST(event_time AS DATE) as date,
            COUNT(*) as daily_events,
            COUNT(DISTINCT user_id) as dau,
            COUNT(DISTINCT user_session) as daily_sessions,
            SUM(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) as views,
            SUM(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) as carts,
            SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) as purchases,
            SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) as daily_revenue
        FROM events
        GROUP BY CAST(event_time AS DATE)
        ORDER BY date
    """)
    return _count(con, "fact_daily_kpis")


def build_user_rfm_segments(con: duckdb.DuckDBPyConnection) -> int:
    """RFM segmentation, recency anchored to the dataset's own max event date.

    Anchoring to CURRENT_DATE would make recency_days (and therefore every
    R-score and every segment label) drift upward by one every day the
    dashboard is deployed, regardless of when the underlying data was
    captured. Anchoring to MAX(event_time) makes the segmentation a pure
    function of the dataset, reproducible on any day it's run.
    """
    con.execute("""
        CREATE OR REPLACE TABLE user_rfm_segments AS
        WITH reference_date AS (
            SELECT MAX(CAST(event_time AS DATE)) as max_date FROM events
        ),
        buyer_rfm AS (
            SELECT
                user_id,
                DATE_DIFF('day', MAX(CAST(event_time AS DATE)), (SELECT max_date FROM reference_date)) as recency_days,
                COUNT(DISTINCT CAST(event_time AS DATE)) as frequency,
                SUM(price) as monetary
            FROM events
            WHERE event_type = 'purchase'
            GROUP BY user_id
        ),
        rfm_scores AS (
            SELECT
                user_id,
                recency_days,
                frequency,
                monetary,
                -- Secondary ORDER BY user_id breaks ties deterministically.
                -- Without it, rows sharing the same recency/frequency/monetary
                -- value can land in different NTILE buckets on different runs
                -- (DuckDB doesn't guarantee stable ordering for ties), so
                -- segment counts would silently shift between rebuilds.
                NTILE(5) OVER (ORDER BY recency_days DESC, user_id) as r_score,
                NTILE(5) OVER (ORDER BY frequency ASC, user_id) as f_score,
                NTILE(5) OVER (ORDER BY monetary ASC, user_id) as m_score
            FROM buyer_rfm
        )
        SELECT
            user_id,
            recency_days,
            frequency,
            monetary,
            r_score,
            f_score,
            m_score,
            r_score + f_score + m_score as rfm_total,
            CASE
                WHEN r_score >= 4 AND f_score >= 4 THEN 'Champions'
                WHEN r_score >= 3 AND f_score >= 3 THEN 'Loyal Customers'
                WHEN r_score >= 4 AND f_score <= 2 THEN 'Promising'
                WHEN r_score <= 2 AND f_score >= 3 THEN 'At Risk'
                WHEN r_score <= 2 AND f_score <= 2 THEN 'Lost'
                ELSE 'Regular'
            END as segment
        FROM rfm_scores
    """)
    return _count(con, "user_rfm_segments")


def build_product_affinity(
    con: duckdb.DuckDBPyConnection, min_support: int = 3, min_lift: float = 1.2
) -> int:
    """Market-basket analysis: co-purchased product pairs with lift > min_lift."""
    con.execute(f"""
        CREATE OR REPLACE TABLE predictions_product_affinity AS
        WITH product_pairs AS (
            SELECT
                a.product_id as product_a,
                b.product_id as product_b,
                COUNT(DISTINCT a.user_session) as pair_count
            FROM events a
            JOIN events b
                ON a.user_session = b.user_session
                AND a.product_id < b.product_id
            WHERE a.event_type = 'purchase'
                AND b.event_type = 'purchase'
            GROUP BY a.product_id, b.product_id
            HAVING COUNT(DISTINCT a.user_session) >= {min_support}
        ),
        product_counts AS (
            SELECT
                product_id,
                COUNT(DISTINCT user_session) as session_count
            FROM events
            WHERE event_type = 'purchase'
            GROUP BY product_id
        ),
        total_sessions AS (
            SELECT COUNT(DISTINCT user_session) as total
            FROM events
            WHERE event_type = 'purchase'
        )
        SELECT
            pp.product_a,
            pp.product_b,
            pp.pair_count,
            pp.pair_count * 1.0 / pa.session_count as confidence,
            (pp.pair_count * 1.0 / pa.session_count) / (pb.session_count * 1.0 / ts.total) as lift
        FROM product_pairs pp
        JOIN product_counts pa ON pp.product_a = pa.product_id
        JOIN product_counts pb ON pp.product_b = pb.product_id
        CROSS JOIN total_sessions ts
        WHERE (pp.pair_count * 1.0 / pa.session_count) / (pb.session_count * 1.0 / ts.total) > {min_lift}
        ORDER BY lift DESC
    """)
    return _count(con, "predictions_product_affinity")


def build_weekly_retention(con: duckdb.DuckDBPyConnection) -> int:
    """Weekly cohort retention, mirroring src/analysis/retention.py's
    `analysis_weekly_retention` logic, which only needs dim_users + events
    (both already available in cloud/sample mode). It was previously
    computed only in the Hydra full-pipeline path, so the dashboard's cohort
    retention heatmap was disabled in cloud/sample mode with a "requires
    pre-computed tables" message. This makes it available there too.
    """
    con.execute("""
        CREATE OR REPLACE TABLE weekly_retention AS
        WITH user_activity AS (
            SELECT
                u.user_id,
                date_trunc('week', u.first_seen) as cohort_week,
                date_trunc('week', e.event_time) as activity_week
            FROM dim_users u
            JOIN events e ON u.user_id = e.user_id
        ),
        cohort_sizes AS (
            SELECT
                date_trunc('week', first_seen) as cohort_week,
                COUNT(DISTINCT user_id) as cohort_size
            FROM dim_users
            GROUP BY 1
        )
        SELECT
            ua.cohort_week,
            cs.cohort_size,
            date_diff('week', ua.cohort_week, ua.activity_week) as weeks_since_first,
            COUNT(DISTINCT ua.user_id) as active_users,
            CAST(COUNT(DISTINCT ua.user_id) AS DOUBLE) / cs.cohort_size as retention_rate
        FROM user_activity ua
        JOIN cohort_sizes cs ON ua.cohort_week = cs.cohort_week
        GROUP BY 1, 2, 3
        ORDER BY 1, 3
    """)
    return _count(con, "weekly_retention")


def build_all(
    con: duckdb.DuckDBPyConnection, logger: logging.Logger | None = None
) -> dict[str, int]:
    """Build the full star schema + ML prediction tables on top of an existing `events` table."""
    counts: dict[str, int] = {}
    steps = [
        ("dim_products", build_dim_products),
        ("dim_users", build_dim_users),
        ("fact_sessions", build_fact_sessions),
        ("fact_daily_kpis", build_fact_daily_kpis),
        ("user_rfm_segments", build_user_rfm_segments),
        ("predictions_product_affinity", build_product_affinity),
        ("weekly_retention", build_weekly_retention),
    ]
    for name, fn in steps:
        if logger:
            logger.info(f"  - Creating {name}...")
        counts[name] = fn(con)
        if logger:
            logger.info(f"    Created {counts[name]:,} rows in {name}")
    return counts
