"""Print every headline figure the README / dashboard cite, straight from the
real full-scale ``data/db/behavior.duckdb`` after the Hydra pipeline + 
``scripts/finalize_full_db.py`` have run. Read-only; safe to re-run.

    python scripts/collect_full_dataset_stats.py
"""
import duckdb, json, textwrap

con = duckdb.connect("data/db/behavior.duckdb", read_only=True)
con.execute("SET memory_limit='4GB'"); con.execute("SET threads TO 6")
out = {}

def one(sql):
    return con.execute(sql).fetchone()

def show(title, df):
    print(f"\n=== {title} ===")
    print(df.to_string(index=False))

# ---- table inventory ----
tabs = [r[0] for r in con.execute("SHOW TABLES").fetchall()]
print("TABLES:", tabs)
for t in tabs:
    print(f"  {t:32s} {one(f'SELECT COUNT(*) FROM {t}')[0]:>14,}")

# ---- events / dataset facts ----
r = one("""SELECT COUNT(*), COUNT(DISTINCT user_id), COUNT(DISTINCT product_id),
        COUNT(DISTINCT user_session), MIN(event_time), MAX(event_time) FROM events""")
out["events"] = dict(zip(["rows","users","products","sessions","min_ts","max_ts"], [str(x) for x in r]))
print("\nEVENTS:", out["events"])
show("event_type", con.execute("SELECT event_type, COUNT(*) n, ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),2) pct FROM events GROUP BY 1 ORDER BY 2 DESC").fetchdf())

# ---- funnel from fact_sessions ----
show("session funnel", con.execute("""
  SELECT COUNT(*) sessions,
         SUM(CAST(has_view AS INT)) s_view,
         SUM(CAST(has_cart AS INT)) s_cart,
         SUM(CAST(has_purchase AS INT)) s_purch,
         ROUND(100.0*SUM(CAST(has_cart AS INT))/NULLIF(SUM(CAST(has_view AS INT)),0),2) view_to_cart_pct,
         ROUND(100.0*SUM(CAST(has_purchase AS INT))/NULLIF(SUM(CAST(has_cart AS INT)),0),2) cart_to_purch_pct,
         ROUND(100.0*SUM(CAST(has_purchase AS INT))/COUNT(*),2) overall_conv_pct,
         ROUND(AVG(duration_sec),1) avg_dur_sec,
         ROUND(AVG(event_count),2) avg_events
  FROM fact_sessions
""").fetchdf())

# ---- revenue / AOV ----
show("revenue", con.execute("""
  SELECT SUM(daily_revenue) total_rev, SUM(purchases) orders,
         SUM(daily_revenue)/NULLIF(SUM(purchases),0) aov FROM fact_daily_kpis
""").fetchdf())

# ---- RFM segments (buyers only) ----
show("analysis_rfm_segments", con.execute("""
  SELECT segment_name, COUNT(*) users,
         ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER(),1) pct_buyers,
         ROUND(AVG(monetary_value),2) avg_spend,
         ROUND(AVG(recency_days),1) avg_recency,
         ROUND(AVG(frequency_count),1) avg_freq
  FROM analysis_rfm_segments GROUP BY 1 ORDER BY 3 DESC
""").fetchdf())

# ---- churn risk ----
show("analysis_churn_risk", con.execute("SELECT status, COUNT(*) n FROM analysis_churn_risk GROUP BY 1 ORDER BY 2 DESC").fetchdf())

# ---- at-risk VIPs: top-decile buyers by spend, showing churn signal ----
show("at-risk VIPs (top spend decile)", con.execute("""
  WITH b AS (
    SELECT r.user_id, r.monetary_value, c.status,
           NTILE(10) OVER (ORDER BY r.monetary_value) AS spend_decile
    FROM analysis_rfm_segments r JOIN analysis_churn_risk c USING(user_id)
  )
  SELECT spend_decile,
         COUNT(*) users,
         ROUND(AVG(monetary_value),2) avg_spend,
         SUM(CASE WHEN status IN ('At Risk','Churned') THEN 1 ELSE 0 END) at_risk_or_churned,
         SUM(CASE WHEN status = 'At Risk' THEN 1 ELSE 0 END) at_risk,
         SUM(CASE WHEN status = 'Churned' THEN 1 ELSE 0 END) churned
  FROM b WHERE spend_decile = 10 GROUP BY 1
""").fetchdf())

# ---- product affinity ----
show("predictions_product_affinity summary", con.execute("""
  SELECT COUNT(*) pairs, ROUND(MIN(lift),3) min_lift, ROUND(MAX(lift),1) max_lift,
         ROUND(AVG(lift),2) avg_lift, MAX(pair_count) max_pair_count
  FROM predictions_product_affinity
""").fetchdf())
show("top affinity pairs", con.execute("""
  SELECT da.brand brand_a, da.category_code cat_a, db.brand brand_b, db.category_code cat_b,
         r.pair_count, ROUND(r.lift,1) lift
  FROM predictions_product_affinity r
  JOIN dim_products da ON r.product_a=da.product_id
  JOIN dim_products db ON r.product_b=db.product_id
  ORDER BY r.pair_count DESC LIMIT 8
""").fetchdf())

# ---- recency -> purchase propensity (the "6x" claim) ----
# For every user active in Oct, bucket by days-since-last-Oct-event; measure Nov purchase rate.
show("recency vs Nov purchase rate", con.execute("""
  WITH oct AS (
    SELECT user_id, date_diff('day', MAX(event_time), DATE '2019-11-01') AS recency_oct
    FROM events WHERE event_time < '2019-11-01' GROUP BY user_id
  ),
  nov AS (SELECT DISTINCT user_id FROM events WHERE event_time >= '2019-11-01' AND event_type='purchase')
  SELECT CASE WHEN recency_oct <= 1 THEN 'a: <=1 day'
              WHEN recency_oct <= 3 THEN 'b: 2-3 days'
              WHEN recency_oct <= 7 THEN 'c: 4-7 days'
              WHEN recency_oct <= 14 THEN 'd: 8-14 days'
              WHEN recency_oct <= 30 THEN 'e: 15-30 days'
              ELSE 'f: >30 days' END AS recency_bucket,
         COUNT(*) users,
         ROUND(100.0*COUNT(nov.user_id)/COUNT(*),3) nov_purchase_rate_pct
  FROM oct LEFT JOIN nov USING(user_id) GROUP BY 1 ORDER BY 1
""").fetchdf())

# ---- weekly retention (week-0 and week-1 avg) ----
show("weekly retention (by weeks_since_first)", con.execute("""
  SELECT weeks_since_first, COUNT(*) cohorts,
         ROUND(AVG(retention_rate)*100,2) avg_retention_pct,
         SUM(active_users) active_users
  FROM analysis_weekly_retention GROUP BY 1 ORDER BY 1 LIMIT 10
""").fetchdf())

con.close()
print("\nDONE")
