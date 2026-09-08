"""Generate the A/B parity fixture the TypeScript port is tested against.

For a set of fixed (control_n, control_conv, treatment_n, treatment_conv, alpha)
cases, this builds the exact DataFrame ABTestEngine.analyze_experiment expects
(k ones and n-k zeros per arm) and records the deterministic statistics it
returns: relative lift, Welch p-value, delta-method 95% CI, and post-hoc power.

The RNG-based synthetic-experiment generator is deliberately not exercised here;
only analyze_experiment is under cross-language parity test (see
deployment_stages.md Phase 5).

Run from the repo root with the dev env installed:

    python frontend/scripts/gen_ab_fixture.py
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from src.analysis.ab_testing import ABTestEngine  # noqa: E402

OUT = Path(__file__).resolve().parent.parent / "src" / "lib" / "__fixtures__" / "ab_cases.json"

# (control_n, control_conv, treatment_n, treatment_conv, confidence_level)
CASES = [
    (2000, 240, 2000, 276, 0.95),
    (2000, 240, 2000, 300, 0.95),
    (1500, 180, 1500, 189, 0.90),
    (8000, 640, 8000, 768, 0.99),
    (500, 60, 500, 66, 0.80),
    (5000, 150, 5000, 225, 0.95),
    (3000, 360, 3000, 360, 0.95),  # zero effect
    (12000, 1440, 12000, 1584, 0.95),
    (1000, 80, 1000, 120, 0.95),  # large lift
    (2845, 304, 2845, 356, 0.95),  # At Risk-ish population
    (2897, 331, 2897, 364, 0.90),
    (700, 84, 700, 92, 0.95),
]


def build_frame(cn: int, ck: int, tn: int, tk: int) -> pd.DataFrame:
    control = np.concatenate([np.ones(ck, dtype=int), np.zeros(cn - ck, dtype=int)])
    treatment = np.concatenate([np.ones(tk, dtype=int), np.zeros(tn - tk, dtype=int)])
    return pd.DataFrame(
        {
            "group": ["control"] * cn + ["treatment"] * tn,
            "converted": np.concatenate([control, treatment]),
        }
    )


def main() -> int:
    out: list[dict[str, object]] = []
    for cn, ck, tn, tk, conf in CASES:
        engine = ABTestEngine(confidence_level=conf)
        frame = build_frame(cn, ck, tn, tk)
        res = engine.analyze_experiment(frame)
        power = float(res["power"])
        power_defined = bool(np.isfinite(power) and 0.0 <= power <= 1.0)
        out.append(
            {
                "input": {
                    "controlN": cn,
                    "controlConversions": ck,
                    "treatmentN": tn,
                    "treatmentConversions": tk,
                    "confidenceLevel": conf,
                },
                "expected": {
                    "relativeLift": float(res["lift"]),
                    "pValue": float(res["p_value"]),
                    "isSignificant": bool(res["stat_sig"]),
                    "ci95Lower": float(res["ci_95"][0]),
                    "ci95Upper": float(res["ci_95"][1]),
                    "power": power if power_defined else None,
                    "powerDefined": power_defined,
                },
            }
        )

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(f"wrote {len(out)} cases -> {OUT.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
