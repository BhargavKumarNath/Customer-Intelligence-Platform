import duckdb
import hydra
from omegaconf import DictConfig
import logging
import time
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
logger = logging.getLogger(__name__)

@hydra.main(version_base=None, config_path="../../config", config_name="config")
def build_recommendations(cfg: DictConfig):
    db_path = cfg.paths.database
    con = duckdb.connect(db_path)
    
    # Moderate memory usage for self-joins
    con.execute("SET memory_limit='12GB';")
    
    try:
        start_global = time.time()
        logger.info("Starting Product Recommendation Engine (Market Basket)...")

        # 1. PREPARE BASKETS
        # Define a "Basket" as products purchased by the same user in the same session.
        # Filter for sessions with > 1 purchase (otherwise no pairs possible).
        
        logger.info("   Identifying multi-item baskets...")
        con.execute("""
        CREATE OR REPLACE TEMP TABLE baskets AS
        SELECT 
            user_session,
            product_id
        FROM events
        WHERE event_type = 'purchase'
        """)
        
        # 2. CALCULATE ITEM PAIRS (Co-occurrence)
        # Self-Join: Find pairs (A, B) appearing in the same session
        # Ensure A < B to avoid duplicate pairs (A,B) and (B,A) for now
        
        logger.info("   Computing product co-occurrences (Self-Join)...")
        query_pairs = """
        CREATE OR REPLACE TEMP TABLE product_pairs AS
        SELECT 
            a.product_id as product_a,
            b.product_id as product_b,
            COUNT(*) as pair_count
        FROM baskets a
        JOIN baskets b ON a.user_session = b.user_session
        WHERE a.product_id != b.product_id
        GROUP BY 1, 2
        HAVING COUNT(*) >= 5; -- Min support threshold to remove noise
        """
        start = time.time()
        con.execute(query_pairs)
        logger.info(f"   Pairs calculated in {time.time() - start:.2f}s")

        # 3. CALCULATE LIFT & CONFIDENCE
        # Lift(A->B) = P(A and B) / (P(A) * P(B))
        # High Lift (>1) means strong association.
        
        logger.info("   Calculating Association Rules (Lift/Confidence)...")
        
        # Get total sessions count for probability calc
        total_sessions = con.execute("SELECT COUNT(DISTINCT user_session) FROM baskets").fetchone()[0]
        
        # Get individual product counts
        con.execute("""
        CREATE OR REPLACE TEMP TABLE product_counts AS
        SELECT product_id, COUNT(*) as cnt 
        FROM baskets GROUP BY 1
        """)
        
        query_rules = f"""
        CREATE OR REPLACE TABLE predictions_product_affinity AS
        SELECT 
            p.product_a,
            p.product_b,
            p.pair_count,
            
            -- Metrics
            (p.pair_count::DOUBLE / pa.cnt) as confidence,
            
            -- Lift Calculation
            -- Lift = (PairProb) / (ProbA * ProbB)
            --      = (pair_count/N) / ((cntA/N) * (cntB/N))
            --      = (pair_count * N) / (cntA * cntB)
            (p.pair_count * {total_sessions}) / (pa.cnt * pb.cnt) as lift
            
        FROM product_pairs p
        JOIN product_counts pa ON p.product_a = pa.product_id
        JOIN product_counts pb ON p.product_b = pb.product_id
        WHERE (p.pair_count * {total_sessions}) / (pa.cnt * pb.cnt) > 1.2 -- Filter for meaningful lift
        ORDER BY lift DESC;
        """
        
        start = time.time()
        con.execute(query_rules)
        row_count = con.execute("SELECT COUNT(*) FROM predictions_product_affinity").fetchone()[0]
        logger.info(f"Recommendation Table built in {time.time() - start:.2f}s ({row_count:,} rules)")

        # 4. ENRICH WITH METADATA FOR PREVIEW
        logger.info("Generating human-readable preview...")
        
        preview_query = """
        SELECT 
            da.brand as brand_a,
            da.category_code as cat_a,
            db.brand as brand_b,
            db.category_code as cat_b,
            r.pair_count,
            ROUND(r.lift, 2) as lift
        FROM predictions_product_affinity r
        JOIN dim_products da ON r.product_a = da.product_id
        JOIN dim_products db ON r.product_b = db.product_id
        ORDER BY r.pair_count DESC
        LIMIT 10;
        """
        
        df = con.execute(preview_query).fetchdf()
        print(df)

    except Exception as e:
        logger.error(f"Error during recommendations: {e}")
    finally:
        con.close()
        logger.info(f"Recommendation pipeline finished in {time.time() - start_global:.2f}s")

if __name__ == "__main__":
    build_recommendations()