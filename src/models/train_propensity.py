import duckdb
import hydra
from omegaconf import DictConfig
import logging
import time
import sys
import json
import subprocess
from datetime import datetime, timezone
import pandas as pd
import numpy as np
import lightgbm as lgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, precision_score, recall_score, confusion_matrix
import pickle

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
logger = logging.getLogger(__name__)

@hydra.main(version_base=None, config_path="../../config", config_name="config")
def train_propensity_model(cfg: DictConfig):
    db_path = cfg.paths.database
    con = duckdb.connect(db_path)
    
    # Memory safety
    con.execute("SET memory_limit='10GB';")
    
    try:
        start_global = time.time()
        logger.info("Starting Propensity Model Training (LightGBM)...")

        # 1. CREATE TRAINING DATASET (Temporal Split)
        # X: Behavior in October (2019-10-01 to 2019-10-31)
        # y: Purchase in November (2019-11-01 to 2019-11-30)
        
        logger.info("   Constructing Training Set (Features: Oct, Target: Nov)...")
        
        query_train = """
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
        
        # Load into Pandas (Should be manageable ~3M rows active in Oct)
        start = time.time()
        df = con.execute(query_train).fetchdf()
        logger.info(f"Training data loaded: {len(df):,} rows in {time.time() - start:.2f}s")
        
        # Check Class Imbalance
        target_counts = df['target'].value_counts()
        logger.info(f"   Class Balance:\n{target_counts}")
        logger.info(f"   Baseline Conversion Rate: {target_counts[1]/len(df):.2%}")

        # 2. TRAIN / TEST SPLIT
        X = df.drop(columns=['user_id', 'target'])
        y = df['target']
        
        # Stratified split to maintain conversion rate in test set
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)
        
        # 3. TRAIN LIGHTGBM
        logger.info("   Training LightGBM Classifier...")
        
        # Create dataset for LightGBM
        train_data = lgb.Dataset(X_train, label=y_train)
        test_data = lgb.Dataset(X_test, label=y_test, reference=train_data)
        
        # Parameters. Deliberately CPU-only (no `device: gpu`), since the project's
        # thesis is that the whole pipeline, including model training, runs on
        # a single laptop without a cloud cluster or a GPU.
        # Seeded explicitly: `feature_fraction` makes LightGBM sample a random
        # subset of features per tree, and without a fixed seed that sampling
        # (and the resulting AUC/feature-importance numbers) differs slightly
        # between otherwise-identical runs on identical data.
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'num_leaves': 31,
            'learning_rate': 0.05,
            'feature_fraction': 0.9,
            'seed': 42,
            'feature_fraction_seed': 42,
            'bagging_seed': 42,
            'data_random_seed': 42,
            'deterministic': True,
        }
        
        # Train with early stopping
        model = lgb.train(
            params,
            train_data,
            valid_sets=[test_data],
            num_boost_round=1000,
            callbacks=[
                lgb.early_stopping(stopping_rounds=50),
                lgb.log_evaluation(100)
            ]
        )
        
        # 4. EVALUATION
        logger.info("Evaluating Model Performance...")
        
        # Predict Probabilities
        y_pred_prob = model.predict(X_test)
        
        # Metrics
        auc = roc_auc_score(y_test, y_pred_prob)
        
        # To calculate Precision/Recall, we need a threshold. Let's pick top 5% as "High Potential"
        threshold = np.percentile(y_pred_prob, 95)
        y_pred_binary = (y_pred_prob >= threshold).astype(int)
        
        precision = precision_score(y_test, y_pred_binary)
        recall = recall_score(y_test, y_pred_binary)
        
        logger.info(f"   AUC-ROC Score: {auc:.4f} (Excellent > 0.8)")
        logger.info(f"   Precision (Top 5%): {precision:.4f} (Of our top picks, how many bought?)")

        # Top-5%-by-score conversion lift over the population baseline. This
        # is the headline number the dashboard's ML Engine page shows.
        baseline_rate = float(y_test.mean())
        top5_rate = float(y_test[y_pred_binary == 1].mean())
        lift = top5_rate / baseline_rate if baseline_rate > 0 else float("nan")
        logger.info(f"   Baseline conversion: {baseline_rate:.4f} | Top-5% conversion: {top5_rate:.4f} | Lift: {lift:.2f}x")

        # Feature Importance
        importance = pd.DataFrame({
            'feature': X.columns,
            'importance': model.feature_importance(importance_type='gain')
        }).sort_values('importance', ascending=False)

        logger.info(f"\nTop Predictive Features:\n{importance.head(5)}")

        # 5. SAVE MODEL & RESULTS
        model_path = "src/models/propensity_lgbm.pkl"
        with open(model_path, 'wb') as f:
            pickle.dump(model, f)
        logger.info(f"Model saved to {model_path}")

        # Metrics artifact: a single source of truth the dashboard (ML
        # Engine page) reads for feature importances and the lift comparison
        # instead of carrying separately hardcoded copies of these numbers
        # that silently drift from what the checked-in model actually does.
        try:
            git_sha = subprocess.check_output(
                ["git", "rev-parse", "--short", "HEAD"], stderr=subprocess.DEVNULL
            ).decode().strip()
        except Exception:
            git_sha = None

        metrics = {
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "git_sha": git_sha,
            "train_rows": int(len(X_train)),
            "test_rows": int(len(X_test)),
            "best_iteration": int(model.best_iteration),
            "auc_roc": round(auc, 4),
            "precision_top5pct": round(float(precision), 4),
            "recall_top5pct": round(float(recall), 4),
            "baseline_conversion_rate": round(baseline_rate, 4),
            "top5pct_conversion_rate": round(top5_rate, 4),
            "lift_top5pct": round(lift, 4),
            "feature_importance_gain": {
                row.feature: float(row.importance) for row in importance.itertuples()
            },
            "params": params,
        }
        metrics_path = "src/models/metrics.json"
        with open(metrics_path, "w") as f:
            json.dump(metrics, f, indent=2)
        logger.info(f"Metrics saved to {metrics_path}")

    except Exception as e:
        logger.error(f"Error during training: {e}")
    finally:
        con.close()
        logger.info(f"Training pipeline finished in {time.time() - start_global:.2f}s")

if __name__ == "__main__":
    train_propensity_model()
