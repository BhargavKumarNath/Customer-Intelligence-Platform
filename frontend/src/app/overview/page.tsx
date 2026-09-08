import type { Metadata } from "next";
import { getMeta, getMetrics } from "@/lib/data";
import { PageHeader, Panel, Section, Tag } from "@/components/ui/primitives";
import { Reveal } from "@/components/ui/reveal";
import { GLOSSARY, STACK } from "@/lib/glossary";
import { integer, shortSha } from "@/lib/format";

export const metadata: Metadata = {
  title: "Method and glossary",
  description:
    "How the platform is built: the offline pipeline, the star schema, the technology choices, and the terms used across the site.",
};

const PIPELINE = [
  {
    step: "Ingest",
    detail:
      "The raw event log is read once through DuckDB and written to a column-trimmed Parquet. Nothing loads the whole table into memory.",
  },
  {
    step: "Star schema",
    detail:
      "build_all() derives dim_users, dim_products, fact_sessions, fact_daily_kpis, plus the RFM, affinity, and retention tables.",
  },
  {
    step: "Score",
    detail:
      "Every user with pre-November activity is scored by the frozen LightGBM model. The bulk pass is prediction-identical to per-user scoring.",
  },
  {
    step: "Mine affinities",
    detail:
      "Product pairs bought in the same session are counted, filtered by support and lift, and keyed for lookup in both directions.",
  },
  {
    step: "Precompute the grid",
    detail:
      "The A/B engine runs across a segment by lift by confidence grid so the experiment page can check its live math against fixed points.",
  },
  {
    step: "Emit artifacts",
    detail:
      "Each output is written as byte-deterministic JSON, keyed by commit. The frontend build pins one set and refuses to build without it.",
  },
];

export default function OverviewPage() {
  const meta = getMeta();
  const metrics = getMetrics();
  const rowCounts = Object.entries(meta.row_counts).sort((a, b) => b[1] - a[1]);

  return (
    <div className="space-y-14">
      <PageHeader
        index="06"
        kicker="How it is built"
        title="Method and glossary"
        lede="No dashboard renders a number it computed itself. Everything traces back to one offline pipeline and one pinned artifact set. This page explains that pipeline and defines the terms."
        meta={
          <>
            <span className="tnum">build {shortSha(meta.git_sha)}</span>
            <span>compiled {new Date(meta.built_at).toISOString().slice(0, 10)}</span>
            <span>model {shortSha(metrics.git_sha)}</span>
          </>
        }
      />

      <Section
        index="01"
        title="The pipeline"
        description="Six stages turn a flat event log into the artifacts this site reads."
      >
        <ol className="space-y-px overflow-hidden rounded-md border border-rule bg-rule">
          {PIPELINE.map((p, i) => (
            <Reveal as="li" key={p.step} delay={i * 0.04}>
              <div className="flex gap-4 bg-surface p-5">
                <span className="tnum shrink-0 text-sm text-accent-ink">
                  {String(i + 1).padStart(2, "0")}
                </span>
                <div>
                  <h3 className="text-sm font-medium text-ink">{p.step}</h3>
                  <p className="mt-1 text-sm leading-relaxed text-ink-muted">{p.detail}</p>
                </div>
              </div>
            </Reveal>
          ))}
        </ol>
        <figure className="mt-6">
          <div className="overflow-x-auto rounded-md border border-rule bg-white p-4">
            {/* eslint-disable-next-line @next/next/no-img-element */}
            <img
              src="/diagrams/system-design.svg"
              alt="End to end system architecture, from raw CSV ingest through DuckDB processing, modeling, and the analytics layer."
              className="mx-auto min-w-[640px] max-w-full"
            />
          </div>
          <figcaption className="mt-2 text-2xs text-ink-faint">
            Source diagram from the pipeline repository.
          </figcaption>
        </figure>
      </Section>

      <Section
        index="02"
        title="The star schema"
        description="Table sizes for this build. Every figure elsewhere on the site aggregates one of these."
      >
        <div className="grid gap-6 lg:grid-cols-[1fr_1.1fr]">
          <Panel title="Row counts" subtitle={`${integer(meta.dataset_rows)} source events`}>
            <dl className="divide-y divide-rule text-sm">
              {rowCounts.map(([table, count]) => (
                <div key={table} className="flex items-center justify-between py-2">
                  <dt className="font-mono text-xs text-ink-muted">{table}</dt>
                  <dd className="tnum text-ink">{integer(count)}</dd>
                </div>
              ))}
            </dl>
          </Panel>
          <figure>
            <div className="overflow-x-auto rounded-md border border-rule bg-white p-4">
              {/* eslint-disable-next-line @next/next/no-img-element */}
              <img
                src="/diagrams/dimensional-model.svg"
                alt="Star schema showing fact tables for sessions and daily KPIs joined to user and product dimensions."
                className="mx-auto min-w-[520px] max-w-full"
              />
            </div>
            <figcaption className="mt-2 text-2xs text-ink-faint">Star schema layout.</figcaption>
          </figure>
        </div>
      </Section>

      <Section
        index="03"
        title="Technology"
        description="Each choice earns its place by removing a moving part, not adding one."
      >
        <div className="overflow-hidden rounded-md border border-rule">
          <table className="w-full text-sm">
            <thead className="bg-surface-sunken text-2xs uppercase tracking-wider text-ink-faint">
              <tr>
                <th className="px-4 py-2 text-left font-medium">Layer</th>
                <th className="px-4 py-2 text-left font-medium">Choice</th>
                <th className="px-4 py-2 text-left font-medium">Why</th>
              </tr>
            </thead>
            <tbody>
              {STACK.map((s) => (
                <tr key={s.layer} className="border-t border-rule/70">
                  <td className="px-4 py-3 align-top text-ink">{s.layer}</td>
                  <td className="px-4 py-3 align-top text-ink-muted">{s.choice}</td>
                  <td className="px-4 py-3 align-top text-ink-muted">{s.reason}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </Section>

      <Section
        index="04"
        title="Glossary"
        description="Terms used across the site, with what each one means for this dataset."
      >
        <div className="space-y-3">
          {(["Data engineering", "Segmentation", "Machine learning", "Experimentation"] as const).map(
            (group) => (
              <div key={group}>
                <h3 className="kicker mb-2">{group}</h3>
                <div className="divide-y divide-rule rounded-md border border-rule">
                  {GLOSSARY.filter((g) => g.group === group).map((entry) => (
                    <details key={entry.term} className="group px-4 py-3">
                      <summary className="flex cursor-pointer items-center justify-between text-sm text-ink marker:content-['']">
                        <span className="font-medium">{entry.term}</span>
                        <span className="flex items-center gap-2 text-2xs text-ink-faint">
                          {entry.full}
                          <span className="transition-transform group-open:rotate-45">+</span>
                        </span>
                      </summary>
                      <div className="mt-2 space-y-2 text-sm leading-relaxed text-ink-muted">
                        <p>{entry.definition}</p>
                        <p className="text-ink-faint">
                          <span className="font-medium text-ink-muted">On this data: </span>
                          {entry.inThisProject}
                        </p>
                      </div>
                    </details>
                  ))}
                </div>
              </div>
            ),
          )}
        </div>
      </Section>

      <Section index="05" title="What this is not">
        <div className="max-w-prose space-y-3 text-sm leading-relaxed text-ink-muted">
          <p>
            <Tag>scope</Tag> The site describes the {meta.date_range[0]} to {meta.date_range[1]}{" "}
            sample: {integer(meta.dataset_rows)} events and {integer(meta.users)} users. It is not the
            full multi-month source, and the aggregate figures scale with that.
          </p>
          <p>
            <Tag>model</Tag> The propensity model is a frozen artifact. Its card on the propensity
            page reports {metrics.auc_roc.toFixed(2)} AUC and {metrics.lift_top5pct.toFixed(1)}x
            top-5% lift as trained, and those values do not change when the sample does.
          </p>
          <p>
            <Tag>freshness</Tag> There is no live data. A new pipeline run produces a new artifact
            set under a new commit, and a redeploy picks it up.
          </p>
        </div>
      </Section>
    </div>
  );
}
