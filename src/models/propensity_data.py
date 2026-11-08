"""The propensity model's training frame and train/test split.

Kept free of Hydra so both train_propensity.py and evaluate_baselines.py
(and their tests) can import it and score exactly the same users.
"""

from __future__ import annotations

import duckdb
import pandas as pd
from sklearn.model_selection import train_test_split

# Features: behaviour in October 2019. Target: any purchase in November 2019.
TRAINING_QUERY = """
WITH oct_behavior AS (
    SELECT 
        user_id,
        COUNT(*) as oct_events,
        COUNT(DISTINCT user_session) as oct_sessions,
        SUM(CASE WHEN event_type='view' THEN 1 ELSE 0 END) as oct_views,
        SUM(CASE WHEN event_type='cart' THEN 1 ELSE 0 END) as oct_carts,
        SUM(CASE WHEN event_type='remove_from_cart' THEN 1 ELSE 0 END) as oct_removes,
        MAX(event_time) as last_oct_event,
        date_diff('day', MIN(event_time), MAX(event_time)) as active_span_days
    FROM events
    WHERE event_time < '2019-11-01'
    GROUP BY user_id
),
nov_outcome AS (
    SELECT 
        user_id,
        1 as converted_in_nov
    FROM events
    WHERE event_time >= '2019-11-01' AND event_type = 'purchase'
    GROUP BY user_id
)
SELECT 
    t.user_id,
    t.oct_events,
    t.oct_sessions,
    t.oct_views,
    t.oct_carts,
    t.oct_removes,
    t.active_span_days,
    -- Recency relative to Oct 31
    date_diff('day', t.last_oct_event, DATE '2019-11-01') as recency_oct,
    -- Target Variable (0 or 1)
    COALESCE(n.converted_in_nov, 0) as target
FROM oct_behavior t
LEFT JOIN nov_outcome n ON t.user_id = n.user_id
ORDER BY t.user_id;
"""
# ORDER BY above matters: without it, DuckDB doesn't guarantee row
# order across runs (especially with multi-threaded execution), so
# even a seeded train_test_split(random_state=42) would shuffle a
# different underlying row order each run and silently produce a
# different actual train/test split every time this script runs.


def load_training_frame(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """One row per user active in October, with the November purchase target."""
    return con.execute(TRAINING_QUERY).fetchdf()


def split_train_test(df: pd.DataFrame):
    """The 80/20 stratified split every propensity number is reported on.

    Shared by training and by evaluate_baselines.py so both score the same
    test users. Returns (X_train, X_test, y_train, y_test).
    """
    features = df.drop(columns=["user_id", "target"])
    y = df["target"]
    return train_test_split(features, y, test_size=0.2, random_state=42, stratify=y)
