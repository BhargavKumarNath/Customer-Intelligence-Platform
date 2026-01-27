import duckdb
import hydra
from omegaconf import DictConfig
import logging
import time
import sys
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
        LEFT JOIN nov_outcome n ON t.user_id = n.user_id;
        """
        
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
        
        # Parameters
        params = {
            'objective': 'binary',
            'metric': 'auc',
            'boosting_type': 'gbdt',
            'num_leaves': 31,
            'learning_rate': 0.05,
            'feature_fraction': 0.9,
            'device': 'gpu',
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

    except Exception as e:
        logger.error(f"Error during training: {e}")
    finally:
        con.close()
        logger.info(f"Training pipeline finished in {time.time() - start_global:.2f}s")

if __name__ == "__main__":
    train_propensity_model()
