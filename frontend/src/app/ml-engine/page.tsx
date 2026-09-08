import type { Metadata } from "next";
import { getAffinity, getMeta, getMetrics } from "@/lib/data";
import {
  PageHeader,
  Panel,
  Section,
  Callout,
  FigureGrid,
  FigureCell,
  Figure,
} from "@/components/ui/primitives";
import { Reveal } from "@/components/ui/reveal";
import { FeatureImportanceChart } from "@/components/ml/feature-importance";
import { PropensityDistribution } from "@/components/ml/propensity-distribution";
import { AffinityExplorer } from "@/components/ml/affinity-explorer";
import { integer, multiple, percent, shortSha } from "@/lib/format";

export const metadata: Metadata = {
  title: "Propensity and affinity",
  description:
    "The LightGBM purchase model: its card, its drivers, the score distribution across users, and the product co-purchase rules.",
};

export default function MlEnginePage() {
  const meta = getMeta();
  const m = getMetrics();
  const affinity = getAffinity();
  const zeroImportanceFeatures = Object.entries(m.feature_importance_gain)
    .filter(([, v]) => v === 0)
    .map(([k]) => k);

  return (
    <div className="space-y-14">
      <PageHeader
        index="03"
        kicker="Where to focus"
        title="Propensity and affinity"
        lede={
          <>
            A gradient-boosted model estimates each user&apos;s chance of buying in November from
            their October behavior. It is useful because the probability it assigns is concentrated:
            a small slice of users accounts for a large share of the expected purchases.
          </>
        }
        meta={
          <>
            <span className="tnum">model {shortSha(m.git_sha)}</span>
            <span>trained {new Date(m.trained_at).toISOString().slice(0, 10)}</span>
            <span>{integer(m.test_rows)} held-out users</span>
          </>
        }
      />

      <Reveal>
        <FigureGrid cols={4}>
          <FigureCell>
            <Figure label="AUC-ROC" value={m.auc_roc.toFixed(3)} sub="held-out ranking" tone="accent" />
          </FigureCell>
          <FigureCell>
            <Figure label="Top 5% lift" value={multiple(m.lift_top5pct)} sub="vs population base" />
          </FigureCell>
          <FigureCell>
            <Figure label="Top 5% conversion" value={percent(m.top5pct_conversion_rate)} sub="predicted slice" />
          </FigureCell>
          <FigureCell>
            <Figure label="Baseline conversion" value={percent(m.baseline_conversion_rate)} sub="all scored users" />
          </FigureCell>
        </FigureGrid>
      </Reveal>

      <Section
        index="01"
        title="What drives a purchase"
        description="Gain-based importance from the frozen model. Share of total gain per feature."
      >
        <div className="grid gap-6 lg:grid-cols-[1.3fr_1fr]">
          <Panel title="Feature importance">
            <FeatureImportanceChart gain={m.feature_importance_gain} />
          </Panel>
          <Callout tone="finding" label="Read" title="Volume and tenure carry the signal">
            The two features that dominate are raw October activity and how long the user has been
            active. Cart adds matter next.{" "}
            {zeroImportanceFeatures.length > 0 && (
              <>
                {zeroImportanceFeatures.join(", ")} contributes zero gain: it was engineered and kept
                for honesty, but the model finds nothing in it on this data.
              </>
            )}
          </Callout>
        </div>
      </Section>

      <Section
        index="02"
        title="Model card"
        description="The frozen artifact, verbatim from metrics.json."
      >
        <div className="grid gap-6 md:grid-cols-2">
          <Panel title="Training and evaluation">
            <dl className="divide-y divide-rule text-sm">
              {[
                ["Trained at", new Date(m.trained_at).toISOString().replace("T", " ").slice(0, 19) + " UTC"],
                ["Train rows", integer(m.train_rows)],
                ["Test rows", integer(m.test_rows)],
                ["Best iteration", integer(m.best_iteration)],
                ["Precision, top 5%", percent(m.precision_top5pct)],
                ["Recall, top 5%", percent(m.recall_top5pct)],
              ].map(([k, v]) => (
                <div key={k} className="flex items-center justify-between py-2">
                  <dt className="text-ink-muted">{k}</dt>
                  <dd className="tnum text-ink">{v}</dd>
                </div>
              ))}
            </dl>
          </Panel>
          <Panel title="Hyperparameters">
            <dl className="divide-y divide-rule text-sm">
              {Object.entries(m.params).map(([k, v]) => (
                <div key={k} className="flex items-center justify-between py-2">
                  <dt className="font-mono text-xs text-ink-muted">{k}</dt>
                  <dd className="tnum text-ink">{String(v)}</dd>
                </div>
              ))}
            </dl>
          </Panel>
        </div>
      </Section>

      <Section
        index="03"
        title="Score distribution"
        description={`Predicted probability for every one of ${integer(meta.propensity_users)} pre-November users.`}
      >
        <Panel>
          <PropensityDistribution baseline={m.baseline_conversion_rate} />
        </Panel>
      </Section>

      <Section
        index="04"
        title="Product affinity"
        description="Co-purchase rules mined from same-session baskets. Pick an anchor to see what pairs with it."
      >
        <Panel>
          <AffinityExplorer affinity={affinity} />
        </Panel>
      </Section>
    </div>
  );
}
