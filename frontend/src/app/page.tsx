import Link from "next/link";
import type { Metadata } from "next";
import {
  getExecutiveSummary,
  getKpis,
  getMeta,
  getMetrics,
  getRfmSummary,
  getWeekOneRetention,
} from "@/lib/data";
import { PageHeader, Callout, Section } from "@/components/ui/primitives";
import { Reveal } from "@/components/ui/reveal";
import { ActionLink } from "@/components/ui/action-link";
import { Headline } from "@/components/home/headline";
import { NAV } from "@/lib/nav";
import { currency, integer, multiple, percent, percentPoints } from "@/lib/format";

export const metadata: Metadata = {
  title: "The read",
  description:
    "Headline findings from a behavioral analytics pass over an e-commerce event log: a retention cliff, lapsed high-value buyers, and where a purchase model concentrates conversions.",
};

export default function HomePage() {
  const meta = getMeta();
  const metrics = getMetrics();
  const kpis = getKpis();
  const summary = getExecutiveSummary();
  const rfm = getRfmSummary();
  const week1 = getWeekOneRetention();

  const atRisk = rfm.find((s) => s.segment === "At Risk");
  const champions = rfm.find((s) => s.segment === "Champions");
  const buyers = rfm.reduce((a, s) => a + s.population, 0);
  const modelLift = metrics.lift_top5pct;

  return (
    <div className="space-y-14">
      <PageHeader
        kicker="Customer Intelligence Platform"
        title={`A behavioral read of ${integer(meta.dataset_rows)} shopping events`}
        lede={
          <>
            This site turns a raw e-commerce event log into the four things a growth team needs to
            act: who the buyers are, whether they come back, which ones are worth targeting, and how
            to test a change before rolling it out. Every number is precomputed from one pinned data
            build.
          </>
        }
        meta={
          <>
            <span>
              {meta.date_range[0]} to {meta.date_range[1]}
            </span>
            <span>{integer(meta.users)} users</span>
            <span>{integer(summary.sessions)} sessions</span>
            <span className="tnum">build {meta.git_sha}</span>
          </>
        }
      />

      <Reveal>
        <Headline
          data={{
            revenue: summary.revenue,
            orders: summary.orders,
            sessionConversion: summary.sessionConversion,
            buyers,
            revenueSeries: kpis.map((k) => k.daily_revenue),
            days: summary.days,
          }}
        />
      </Reveal>

      <Section
        index="00"
        title="What the data says"
        description="Three findings carry most of the weight. Each links to the section that works through it."
      >
        <div className="grid gap-4 lg:grid-cols-3">
          <Reveal delay={0}>
            <Callout
              tone="critical"
              label="Finding 01 · Retention"
              title="Two in three first-week visitors do not come back the next week"
              action={<ActionLink href="/user-intelligence">See the cohort grid</ActionLink>}
            >
              Week-one retention across all acquisition cohorts is {percent(week1)}. The drop happens
              immediately after the first visit, which points at a discovery problem rather than a
              churn problem later in the lifecycle.
            </Callout>
          </Reveal>

          <Reveal delay={0.06}>
            <Callout
              tone="caution"
              label="Finding 02 · Lapsed value"
              title={
                atRisk
                  ? `${currency(atRisk.total_monetary)} sits with buyers who have gone quiet`
                  : "A high-value segment has gone quiet"
              }
              action={<ActionLink href="/experiments">Design the reactivation test</ActionLink>}
            >
              {atRisk ? (
                <>
                  The <em>At Risk</em> segment is {integer(atRisk.population)} buyers who used to
                  purchase often and now average {Math.round(atRisk.avg_recency_days)} days since
                  their last order. Their lifetime spend is {currency(atRisk.total_monetary)}. That is
                  the population to run a reactivation experiment against.
                </>
              ) : (
                <>The RFM breakdown flags a segment of lapsed frequent buyers worth reactivating.</>
              )}
            </Callout>
          </Reveal>

          <Reveal delay={0.12}>
            <Callout
              tone="finding"
              label="Finding 03 · Targeting"
              title={`The purchase model concentrates ${multiple(modelLift)} the conversion rate in its top 5%`}
              action={<ActionLink href="/ml-engine">Open the propensity model</ActionLink>}
            >
              Against a {percent(metrics.baseline_conversion_rate)} baseline, the top five percent of
              users by predicted propensity convert at{" "}
              {percent(metrics.top5pct_conversion_rate)} on held-out data (AUC{" "}
              {metrics.auc_roc.toFixed(2)}). Spend aimed at that slice does roughly{" "}
              {Math.round((modelLift - 1) * 100)}% more work per contact.
            </Callout>
          </Reveal>
        </div>
      </Section>

      <Section
        index="01"
        title="How to read this site"
        description="The sections are ordered as an argument, from what happened to what to do next."
      >
        <ol className="divide-y divide-rule border-y border-rule">
          {NAV.filter((n) => n.href !== "/").map((item) => (
            <li key={item.href}>
              <Link
                href={item.href}
                className="group flex items-start gap-4 py-4 transition-colors hover:bg-surface-sunken/50"
              >
                <span className="tnum mt-0.5 text-xs text-accent-ink">{item.index}</span>
                <span className="flex-1">
                  <span className="block text-sm font-medium text-ink group-hover:text-accent-ink">
                    {item.title}
                  </span>
                  <span className="mt-0.5 block text-sm text-ink-muted">{item.summary}</span>
                </span>
              </Link>
            </li>
          ))}
        </ol>
      </Section>

      <Section
        index="02"
        title="Provenance"
        description="What you are looking at, and what you are not."
      >
        <div className="prose-sm max-w-prose space-y-3 text-sm leading-relaxed text-ink-muted">
          <p>
            The source is a public dataset of shopping events from October and November 2019. The
            numbers here describe that window: {integer(meta.dataset_rows)} events,{" "}
            {integer(meta.users)} users, {integer(summary.sessions)} sessions, and{" "}
            {currency(summary.revenue)} in purchase-event revenue.
          </p>
          <p>
            The build pipeline constructs a star schema, scores every pre-November user with a frozen
            LightGBM model, mines product co-purchase rules, and precomputes an A/B grid. The
            frontend reads the output and renders it. There is no server on the request path, and{" "}
            {champions ? `the ${integer(champions.population)} Champions` : "no segment"} figure, like
            every other, comes straight from that build.
          </p>
        </div>
        <div className="mt-4">
          <ActionLink href="/overview">Read the method and glossary</ActionLink>
        </div>
      </Section>

      <p className="hairline pt-6 text-2xs text-ink-faint">
        Segment share of buyers sums to {percentPoints(rfm.reduce((a, s) => a + s.pct_of_buyers, 0), 0)}.
      </p>
    </div>
  );
}
