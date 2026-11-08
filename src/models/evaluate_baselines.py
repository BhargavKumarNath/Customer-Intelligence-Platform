"""How much of the propensity model's lift is LightGBM, and how sure are we?

Scores the checked-in LightGBM model against two cheaper rankers on the
same held-out users it was evaluated on, and puts 95% bootstrap intervals
on every number:

  lightgbm   src/models/propensity_lgbm.txt (the model the dashboard serves)
  logistic   logistic regression on log1p-scaled versions of the same 7
             features, fit on the same training users
  recency    no model: rank users by days since their last October event

The comparison is fixed before any of it is run: same test users, same
"top 5% of scored users" cut-off, the same bootstrap resamples for every
ranker (so differences are paired), 1,000 resamples, seed 42. The script
refuses to run unless the LightGBM AUC it measures matches the AUC recorded
in src/models/metrics.json, which proves it is scoring the same test set.

Ties: a recency score of "1 day" is shared by hundreds of thousands of
users, so "the top 5%" has to split a tie group. Every ranker's top-5% rate
here is the expected rate under a random order within the boundary tie
group (full groups above the cut plus the needed fraction of the boundary
group at that group's mean rate). The headline lift in metrics.json uses
``score >= 95th percentile`` instead; that number is reproduced as-is in
``lightgbm_headline`` so nothing silently changes definition.

Needs the full dataset in data/db/behavior.duckdb (see the readme). Run:

    python -m src.models.evaluate_baselines
"""

from __future__ import annotations

import argparse
import json
import logging
import math
from datetime import UTC, datetime
from pathlib import Path

import duckdb
import lightgbm as lgb
import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import FunctionTransformer, StandardScaler

from src.models.propensity_data import load_training_frame, split_train_test

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = ROOT / "data" / "db" / "behavior.duckdb"
MODEL_PATH = ROOT / "src" / "models" / "propensity_lgbm.txt"
METRICS_PATH = ROOT / "src" / "models" / "metrics.json"
OUT_PATH = ROOT / "src" / "models" / "baselines.json"

TOP_FRAC = 0.05
N_BOOT = 1_000
SEED = 42
LEVEL = 0.95


def top_frac_rate(scores: np.ndarray, y: np.ndarray, frac: float = TOP_FRAC) -> float:
    """Conversion rate among the top ``frac`` of users by score, tie-aware.

    The boundary tie group contributes the fraction of it that fits, at its
    own mean rate: the expected value under random ordering within ties.
    """
    n = scores.size
    k = frac * n
    order = np.argsort(-scores, kind="stable")
    s_sorted = scores[order]
    y_sorted = y[order]
    # Group boundaries of equal scores in descending order.
    change = np.flatnonzero(np.diff(s_sorted)) + 1
    starts = np.concatenate(([0], change))
    ends = np.concatenate((change, [n]))
    group_pos = np.cumsum(y_sorted)
    taken = 0.0
    positives = 0.0
    for a, b in zip(starts, ends, strict=True):
        size = b - a
        group_y = group_pos[b - 1] - (group_pos[a - 1] if a > 0 else 0)
        if taken + size <= k:
            taken += size
            positives += group_y
            if taken == k:
                break
        else:
            need = k - taken
            positives += need * group_y / size
            taken = k
            break
    return positives / k


def headline_top5_rate(scores: np.ndarray, y: np.ndarray) -> float:
    """The rule train_propensity.py uses: score >= 95th percentile."""
    threshold = np.percentile(scores, 95)
    return float(y[scores >= threshold].mean())


def ci(values: np.ndarray) -> dict[str, float]:
    alpha = (1 - LEVEL) / 2
    lo, hi = np.quantile(values, [alpha, 1 - alpha])
    return {"lower": float(lo), "upper": float(hi)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--n-boot", type=int, default=N_BOOT)
    args = parser.parse_args()

    recorded = json.loads(METRICS_PATH.read_text())
    con = duckdb.connect(str(args.db), read_only=True)
    try:
        n_events = con.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        df = load_training_frame(con)
    finally:
        con.close()
    x_train, x_test, y_train, y_test = split_train_test(df)
    del df
    y = y_test.to_numpy().astype(np.int8)
    logger.info(
        "events=%s train=%s test=%s", f"{n_events:,}", f"{len(x_train):,}", f"{len(x_test):,}"
    )
    if len(x_train) != recorded["train_rows"] or len(x_test) != recorded["test_rows"]:
        raise SystemExit(
            f"Split size {len(x_train)}/{len(x_test)} differs from metrics.json "
            f"{recorded['train_rows']}/{recorded['test_rows']}: not the same data."
        )

    booster = lgb.Booster(model_file=str(MODEL_PATH))
    scores = {"lightgbm": booster.predict(x_test[booster.feature_name()])}
    lgb_auc = roc_auc_score(y, scores["lightgbm"])
    if round(lgb_auc, 4) != recorded["auc_roc"]:
        raise SystemExit(
            f"LightGBM AUC {lgb_auc:.4f} != recorded {recorded['auc_roc']}: not the same test set."
        )

    logistic = make_pipeline(
        FunctionTransformer(np.log1p),  # counts and day spans are heavy-tailed and >= 0
        StandardScaler(),
        LogisticRegression(max_iter=2_000),
    )
    logistic.fit(x_train, y_train)
    scores["logistic"] = logistic.predict_proba(x_test)[:, 1]
    scores["recency"] = -x_test["recency_oct"].to_numpy(dtype=np.float64)
    del x_train, y_train

    base_rate = float(y.mean())
    point: dict[str, dict[str, float]] = {}
    for name, s in scores.items():
        rate = top_frac_rate(s, y)
        point[name] = {
            "auc": float(roc_auc_score(y, s)),
            "top5_rate": rate,
            "lift": rate / base_rate,
        }
        logger.info("%-9s AUC %.4f  top-5%% rate %.4f  lift %.3fx", name, *point[name].values())
    headline_rate = headline_top5_rate(scores["lightgbm"], y)

    rng = np.random.default_rng(SEED)
    n = y.size
    boot: dict[str, dict[str, list[float]]] = {k: {"auc": [], "lift": []} for k in scores}
    boot_base: list[float] = []
    for i in range(args.n_boot):
        idx = rng.integers(0, n, n)
        yb = y[idx]
        br = yb.mean()
        boot_base.append(float(br))
        for name, s in scores.items():
            sb = s[idx]
            boot[name]["auc"].append(float(roc_auc_score(yb, sb)))
            boot[name]["lift"].append(top_frac_rate(sb, yb) / br)
        if (i + 1) % 100 == 0:
            logger.info("bootstrap %d/%d", i + 1, args.n_boot)

    results: dict[str, dict] = {}
    for name in scores:
        auc_b = np.array(boot[name]["auc"])
        lift_b = np.array(boot[name]["lift"])
        results[name] = {
            "auc": point[name]["auc"],
            "auc_ci95": ci(auc_b),
            "top5_rate": point[name]["top5_rate"],
            "lift": point[name]["lift"],
            "lift_ci95": ci(lift_b),
        }
        if name != "lightgbm":
            d_auc = np.array(boot["lightgbm"]["auc"]) - auc_b
            d_lift = np.array(boot["lightgbm"]["lift"]) - lift_b
            results[name]["lightgbm_minus_this"] = {
                "auc": point["lightgbm"]["auc"] - point[name]["auc"],
                "auc_ci95": ci(d_auc),
                "lift": point["lightgbm"]["lift"] - point[name]["lift"],
                "lift_ci95": ci(d_lift),
            }

    record = {
        "measured_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "data": {
            "events": int(n_events),
            "train_users": int(recorded["train_rows"]),
            "test_users": int(n),
        },
        "baseline_conversion_rate": base_rate,
        "top_fraction": TOP_FRAC,
        "bootstrap": {"n_boot": args.n_boot, "seed": SEED, "level": LEVEL, "paired": True},
        "lightgbm_headline": {
            "rule": "score >= 95th percentile (train_propensity.py)",
            "auc": lgb_auc,
            "top5_rate": headline_rate,
            "lift": headline_rate / base_rate,
            "matches_metrics_json": math.isclose(
                round(headline_rate / base_rate, 3), round(recorded["lift_top5pct"], 3)
            ),
        },
        "rankers": results,
        "logistic_spec": "log1p -> StandardScaler -> LogisticRegression(max_iter=2000), fit on train users",
        "recency_spec": "score = -recency_oct (fewer days since last October event ranks higher)",
    }
    OUT_PATH.write_text(json.dumps(record, indent=2) + "\n")
    logger.info("wrote %s", OUT_PATH)


if __name__ == "__main__":
    main()
