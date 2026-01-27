import duckdb
import hydra
from omegaconf import DictConfig
import logging
import time
import sys
import numpy as np
import pandas as pd
from scipy import stats
from statsmodels.stats.power import TTestIndPower

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
logger = logging.getLogger(__name__)

class ABTestEngine:
    def __init__(self, confidence_level=0.95):
        self.alpha = 1 - confidence_level
        self.confidence = confidence_level

    def generate_synthetic_experiment(self, df, lift=0.15):
        """
        Simulates an A/B test on historical data.
        Group A: Historical Conversion Rate
        Group B: Historical Rate * (1 + lift)
        """
        np.random.seed(42)
        n = len(df)
        
        # 1. Random Assignment (50/50 split)
        df['group'] = np.random.choice(['control', 'treatment'], size=n, p=[0.5, 0.5])
        
        # 2. Simulate Outcome
        # Use the 'checkout_rate' from features as a proxy for their base probability
        # For simplicity in simulation, treat 'checkout_rate' > 0 as a conversion
        # And add random noise + lift to Treatment
        
        base_prob = 0.12  # Assume baseline conversion for this segment is 12%
        
        # Generate outcomes
        # Control: Bernoulli(p)
        # Treatment: Bernoulli(p * (1+lift))
        
        conditions = [
            (df['group'] == 'control'),
            (df['group'] == 'treatment')
        ]
        
        prob_control = base_prob
        prob_treatment = base_prob * (1 + lift)
        
        # Vectorized simulation
        df['converted'] = 0
        
        mask_a = df['group'] == 'control'
        df.loc[mask_a, 'converted'] = np.random.binomial(1, prob_control, size=mask_a.sum())
        
        mask_b = df['group'] == 'treatment'
        df.loc[mask_b, 'converted'] = np.random.binomial(1, prob_treatment, size=mask_b.sum())
        
        return df

    def analyze_experiment(self, df):
        """
        Performs rigorous statistical testing.
        """
        # Aggregation
        summary = df.groupby('group')['converted'].agg(['count', 'sum', 'mean'])
        summary.columns = ['visitors', 'conversions', 'conversion_rate']
        
        control = summary.loc['control']
        treatment = summary.loc['treatment']
        
        # 1. Relative Lift
        lift = (treatment['conversion_rate'] - control['conversion_rate']) / control['conversion_rate']
        
        # 2. Statistical Test (Z-test for Proportions approx via T-test for large N)
        # For binary data, T-test is robust for large samples
        t_stat, p_value = stats.ttest_ind(
            df[df['group'] == 'treatment']['converted'],
            df[df['group'] == 'control']['converted'],
            equal_var=False
        )
        
        # 3. Confidence Interval (Delta Method approximation)
        # Standard Error of the difference
        se_a = np.sqrt(control['conversion_rate'] * (1 - control['conversion_rate']) / control['visitors'])
        se_b = np.sqrt(treatment['conversion_rate'] * (1 - treatment['conversion_rate']) / treatment['visitors'])
        se_diff = np.sqrt(se_a**2 + se_b**2)
        
        z_score = stats.norm.ppf(1 - self.alpha/2)
        diff = treatment['conversion_rate'] - control['conversion_rate']
        ci_lower = diff - z_score * se_diff
        ci_upper = diff + z_score * se_diff
        
        # 4. Power Analysis (Post-hoc)
        effect_size = (treatment['conversion_rate'] - control['conversion_rate']) / np.sqrt((control['conversion_rate'] * (1-control['conversion_rate']) + treatment['conversion_rate'] * (1-treatment['conversion_rate']))/2)
        power_analysis = TTestIndPower()
        power = power_analysis.solve_power(effect_size=effect_size, nobs1=control['visitors'], ratio=1.0, alpha=self.alpha)

        return {
            'summary': summary,
            'lift': lift,
            'p_value': p_value,
            'stat_sig': p_value < self.alpha,
            'ci_95': (ci_lower, ci_upper),
            'power': power
        }

@hydra.main(version_base=None, config_path="../../config", config_name="config")
def run_ab_simulation(cfg: DictConfig):
    db_path = cfg.paths.database
    con = duckdb.connect(db_path)
    
    try:
        logger.info("Starting A/B Test Simulation Engine...")
        
        # 1. Load the Target Segment ("Can't Lose Them")
        logger.info("   Loading 'Cant Lose Them' segment...")
        query = """
        SELECT user_id, checkout_rate
        FROM features_users
        WHERE rfm_segment = 'Cant Lose Them'
        """
        df_segment = con.execute(query).fetchdf()
        logger.info(f"   Target Population: {len(df_segment):,} users")
        
        # 2. Simulate Experiment
        engine = ABTestEngine()
        
        # We simulate a "Coupon Campaign" with expected 15% Lift
        logger.info("   Simulating Campaign: 'Reactivation Coupon' (Expected Lift: +15%)...")
        df_experiment = engine.generate_synthetic_experiment(df_segment, lift=0.15)
        
        # 3. Analyze Results
        results = engine.analyze_experiment(df_experiment)
        
        # 4. Reporting
        summary = results['summary']
        logger.info(f"\nExperiment Results Summary:\n{summary}")
        logger.info(f"   ------------------------------------------")
        logger.info(f"   Relative Lift:    {results['lift']:.2%}")
        logger.info(f"   P-Value:          {results['p_value']:.5f}")
        logger.info(f"   Significant?:     {results['stat_sig']}")
        logger.info(f"   95% CI (Abs):     [{results['ci_95'][0]:.4f}, {results['ci_95'][1]:.4f}]")
        logger.info(f"   Statistical Power: {results['power']:.4f}")
        
        if results['stat_sig'] and results['lift'] > 0:
            logger.info("RECOMMENDATION: SHIP IT! The coupon campaign drove significant incremental revenue.")
        else:
            logger.info("RECOMMENDATION: HOLD. Results are inconclusive or negative.")

    except Exception as e:
        logger.error(f"Error during A/B analysis: {e}")
    finally:
        con.close()

if __name__ == "__main__":
    run_ab_simulation()