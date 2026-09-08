import type { Metadata } from "next";
import { getExecutiveSummary, getKpis, getMeta } from "@/lib/data";
import { PageHeader, Panel, Section, Callout, FigureGrid, FigureCell, Figure } from "@/components/ui/primitives";
import { Reveal } from "@/components/ui/reveal";
import { Funnel } from "@/components/charts/funnel";
import { KpiTrends, WeekdayRhythm } from "@/components/executive/executive-charts";
import { currency, integer, percent } from "@/lib/format";

export const metadata: Metadata = {
  title: "Business performance",
  description:
    "Revenue, orders, average order value, and the view-to-purchase funnel across the tracked window, computed from daily KPI rows.",
};

export default function ExecutivePage() {
  const meta = getMeta();
  const kpis = getKpis();
  const s = getExecutiveSummary();

  return (
    <div className="space-y-14">
      <PageHeader
        index="01"
        kicker="What happened"
        title="Business performance"
        lede={
          <>
            {s.days} days of trading, summarised from the daily KPI table. The headline is a healthy
            top of funnel and a purchase step that does most of the filtering.
          </>
        }
        meta={
          <>
            <span>
              {meta.date_range[0]} to {meta.date_range[1]}
            </span>
            <span>{integer(s.events)} events</span>
            <span>{integer(s.sessions)} sessions</span>
          </>
        }
      />

      <Reveal>
        <FigureGrid cols={4}>
          <FigureCell>
            <Figure label="Total revenue" value={currency(s.revenue)} sub={`${s.days} trading days`} />
          </FigureCell>
          <FigureCell>
            <Figure label="Purchases" value={integer(s.orders)} sub="purchase events" />
          </FigureCell>
          <FigureCell>
            <Figure label="Average order" value={currency(s.aov)} sub="revenue over purchases" />
          </FigureCell>
          <FigureCell>
            <Figure
              label="Session conversion"
              value={percent(s.sessionConversion, 2)}
              sub="purchases over sessions"
              tone="accent"
            />
          </FigureCell>
        </FigureGrid>
      </Reveal>

      <Section
        index="01"
        title="Revenue and traffic"
        description="Daily series from fact_daily_kpis. Switch the metric to compare the shapes."
      >
        <Panel bodyClassName="pt-5">
          <KpiTrends kpis={kpis} />
        </Panel>
      </Section>

      <Section
        index="02"
        title="The funnel"
        description="Event counts across the log, not session-distinct. The ratios still show where volume is lost."
      >
        <div className="grid gap-6 lg:grid-cols-[1.3fr_1fr]">
          <Panel title="View to purchase">
            <Funnel
              stages={[
                { label: "Product views", value: s.views },
                { label: "Cart adds", value: s.carts },
                { label: "Purchases", value: s.orders },
              ]}
            />
          </Panel>
          <Callout
            tone="caution"
            label="Read"
            title="The catalogue gets seen. The cart is where intent thins out."
          >
            {percent(s.viewToCart, 1)} of views become a cart add, and {percent(s.cartToOrder, 1)} of
            cart adds become a purchase. The first ratio is the one to move: more of the work is
            upstream of the cart than inside it.
          </Callout>
        </div>
      </Section>

      <Section
        index="03"
        title="Weekly rhythm"
        description="Average revenue by day of week across the window."
      >
        <Panel>
          <WeekdayRhythm kpis={kpis} />
        </Panel>
      </Section>

      <p className="hairline pt-6 text-2xs text-ink-faint">
        Peak day drew {integer(s.peakDau)} active users. All figures aggregate {kpis.length} rows of
        fact_daily_kpis.
      </p>
    </div>
  );
}
