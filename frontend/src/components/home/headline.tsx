"use client";

import { CountUp } from "@/components/ui/count-up";
import { Sparkline } from "@/components/charts/sparkline";
import { FigureCell, FigureGrid } from "@/components/ui/primitives";
import { compactNumber, currency, integer, percent } from "@/lib/format";

export interface HeadlineData {
  revenue: number;
  orders: number;
  sessionConversion: number;
  buyers: number;
  revenueSeries: number[];
  days: number;
}

export function Headline({ data }: { data: HeadlineData }) {
  return (
    <FigureGrid cols={4}>
      <FigureCell>
        <span className="kicker">Revenue tracked</span>
        <div className="tnum mt-2 text-3xl text-ink">
          <CountUp value={data.revenue} format={(n) => currency(n)} />
        </div>
        <div className="mt-2 text-accent">
          <Sparkline values={data.revenueSeries} width={132} height={30} strokeClass="stroke-accent" />
        </div>
        <span className="mt-1 block text-2xs text-ink-faint">across {data.days} days</span>
      </FigureCell>

      <FigureCell>
        <span className="kicker">Purchase events</span>
        <div className="tnum mt-2 text-3xl text-ink">
          <CountUp value={data.orders} format={(n) => integer(n)} />
        </div>
        <span className="mt-2 block text-2xs text-ink-faint">
          {currency(data.orders > 0 ? data.revenue / data.orders : 0)} average order
        </span>
      </FigureCell>

      <FigureCell>
        <span className="kicker">Session conversion</span>
        <div className="tnum mt-2 text-3xl text-ink">
          <CountUp value={data.sessionConversion * 100} format={(n) => `${n.toFixed(2)}%`} />
        </div>
        <span className="mt-2 block text-2xs text-ink-faint">purchases over sessions</span>
      </FigureCell>

      <FigureCell>
        <span className="kicker">Segmented buyers</span>
        <div className="tnum mt-2 text-3xl text-ink">
          <CountUp value={data.buyers} format={(n) => compactNumber(n)} />
        </div>
        <span className="mt-2 block text-2xs text-ink-faint">placed in an RFM segment</span>
      </FigureCell>
    </FigureGrid>
  );
}

export { percent };
