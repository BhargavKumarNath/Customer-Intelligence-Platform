import type { Metadata } from "next";
import { getMeta, getRetention, getRfmSummary, getWeekOneRetention } from "@/lib/data";
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
import { CohortHeatmap } from "@/components/charts/cohort-heatmap";
import { RfmScatter } from "@/components/user-intel/rfm-scatter";
import { SegmentPopulationBars } from "@/components/user-intel/segment-bars";
import { UserLookup } from "@/components/user-intel/user-lookup";
import { currency, integer, percent, percentPoints } from "@/lib/format";

export const metadata: Metadata = {
  title: "User intelligence",
  description:
    "RFM segments with per-segment economics, a retention cohort grid, and a single-user lookup joining segment and propensity.",
};

export default function UserIntelligencePage() {
  const meta = getMeta();
  const rfm = getRfmSummary().slice().sort((a, b) => b.population - a.population);
  const retention = getRetention();
  const week1 = getWeekOneRetention();
  const buyers = rfm.reduce((a, s) => a + s.population, 0);
  const atRisk = rfm.find((s) => s.segment === "At Risk");
  const champions = rfm.find((s) => s.segment === "Champions");

  const popBars = rfm.map((s, i) => ({
    label: s.segment,
    value: s.population,
    colorIndex: i,
    meta: `${percentPoints(s.pct_of_buyers)} of buyers`,
  }));

  return (
    <div className="space-y-14">
      <PageHeader
        index="02"
        kicker="Who is buying"
        title="User intelligence"
        lede={
          <>
            Buyers split into six behavioral segments by recency and frequency. The retention grid
            then shows how quickly first-time visitors fall away, which is the real constraint on
            growth here.
          </>
        }
        meta={
          <>
            <span>{integer(buyers)} segmented buyers</span>
            <span>{rfm.length} segments</span>
            <span>{integer(meta.users)} users total</span>
          </>
        }
      />

      <Reveal>
        <FigureGrid cols={4}>
          <FigureCell>
            <Figure label="Segmented buyers" value={integer(buyers)} sub={`of ${integer(meta.users)} users`} />
          </FigureCell>
          <FigureCell>
            <Figure
              label="Week 1 retention"
              value={percent(week1)}
              sub="cohort-weighted"
              tone="accent"
            />
          </FigureCell>
          <FigureCell>
            <Figure
              label="At Risk value"
              value={atRisk ? currency(atRisk.total_monetary) : "n/a"}
              sub={atRisk ? `${integer(atRisk.population)} buyers` : undefined}
            />
          </FigureCell>
          <FigureCell>
            <Figure
              label="Champions"
              value={champions ? integer(champions.population) : "n/a"}
              sub={champions ? `avg spend ${currency(champions.avg_monetary)}` : undefined}
            />
          </FigureCell>
        </FigureGrid>
      </Reveal>

      <Section
        index="01"
        title="Segments"
        description="Population and per-segment economics from user_rfm_segments."
      >
        <div className="grid gap-6 lg:grid-cols-[1fr_1.4fr]">
          <Panel title="Buyers per segment">
            <SegmentPopulationBars data={popBars} />
          </Panel>
          <Panel title="Per-segment profile" bodyClassName="p-0">
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead className="bg-surface-sunken text-2xs uppercase tracking-wider text-ink-faint">
                  <tr>
                    <th className="px-4 py-2 text-left font-medium">Segment</th>
                    <th className="px-4 py-2 text-right font-medium">Buyers</th>
                    <th className="px-4 py-2 text-right font-medium">Avg recency</th>
                    <th className="px-4 py-2 text-right font-medium">Avg spend</th>
                    <th className="px-4 py-2 text-right font-medium">Total spend</th>
                  </tr>
                </thead>
                <tbody>
                  {rfm.map((s) => (
                    <tr key={s.segment} className="border-t border-rule/70">
                      <td className="px-4 py-2.5 text-ink">{s.segment}</td>
                      <td className="tnum px-4 py-2.5 text-right text-ink-muted">{integer(s.population)}</td>
                      <td className="tnum px-4 py-2.5 text-right text-ink-muted">
                        {Math.round(s.avg_recency_days)}d
                      </td>
                      <td className="tnum px-4 py-2.5 text-right text-ink-muted">
                        {currency(s.avg_monetary)}
                      </td>
                      <td className="tnum px-4 py-2.5 text-right text-ink">{currency(s.total_monetary)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Panel>
        </div>
      </Section>

      <Section
        index="02"
        title="Segment landscape"
        description="Every buyer placed by recency and lifetime spend. Clusters correspond to the named segments."
      >
        <Panel>
          <RfmScatter />
        </Panel>
      </Section>

      <Section
        index="03"
        title="Retention cohorts"
        description="Each row is the week a user first appeared. Cells are the share still active in that later week."
      >
        <Panel bodyClassName="p-4">
          <CohortHeatmap rows={retention} />
        </Panel>
        <div className="mt-4">
          <Callout
            tone="critical"
            label="Read"
            title={`The drop is immediate: week 1 retention is ${percent(week1)}`}
          >
            After the first week the curve flattens, so the users who stay tend to keep coming back.
            The loss is concentrated in the first seven days, which is a first-visit experience
            problem more than a loyalty problem.
          </Callout>
        </div>
      </Section>

      <Section
        index="04"
        title="Look up a user"
        description="Join one user's RFM segment to their propensity score."
      >
        <UserLookup />
      </Section>
    </div>
  );
}
