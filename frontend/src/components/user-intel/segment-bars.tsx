"use client";

import { HorizontalBars, type BarDatum } from "@/components/charts/bar-series";
import { integer } from "@/lib/format";

export function SegmentPopulationBars({ data }: { data: BarDatum[] }) {
  return <HorizontalBars data={data} valueFormat={(v) => integer(v)} />;
}
