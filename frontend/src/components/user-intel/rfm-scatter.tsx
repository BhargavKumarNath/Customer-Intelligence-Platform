"use client";

import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchSegments } from "@/lib/client-data";
import { GroupedScatter, type ScatterPoint } from "@/components/charts/scatter";
import { ChartSkeleton, ErrorState } from "@/components/ui/states";
import { currency, integer } from "@/lib/format";

const SEGMENT_ORDER = ["Champions", "Loyal Customers", "Promising", "Regular", "At Risk", "Lost"];
const SAMPLE_TARGET = 2600;

export function RfmScatter() {
  const query = useQuery({
    queryKey: ["segments"],
    queryFn: ({ signal }) => fetchSegments(signal),
  });

  const { points, groups, shown, total } = useMemo(() => {
    if (!query.data) return { points: [] as ScatterPoint[], groups: [] as string[], shown: 0, total: 0 };
    const all = Object.values(query.data).filter((u) => u.monetary > 0 && u.recency_days >= 0);
    const stride = Math.max(1, Math.floor(all.length / SAMPLE_TARGET));
    const sample = all.filter((_, i) => i % stride === 0);
    const present = SEGMENT_ORDER.filter((g) => sample.some((u) => u.segment === g));
    return {
      points: sample.map((u) => ({
        x: u.recency_days,
        y: u.monetary,
        z: u.frequency,
        group: u.segment,
      })),
      groups: present,
      shown: sample.length,
      total: all.length,
    };
  }, [query.data]);

  if (query.isPending) return <ChartSkeleton height={420} />;
  if (query.isError) {
    return (
      <ErrorState title="Could not load the segment file" onRetry={() => query.refetch()}>
        segments.json did not load. It is fetched only on this page.
      </ErrorState>
    );
  }

  return (
    <div>
      <GroupedScatter
        points={points}
        groups={groups}
        xLabel="Days since last order"
        yLabel="Lifetime spend"
        xFormat={(v) => integer(v)}
        yFormat={(v) => currency(v, { compact: true })}
        yScale="log"
        height={440}
      />
      <p className="mt-3 text-2xs text-ink-faint">
        Showing {integer(shown)} of {integer(total)} buyers, sampled evenly. Point size is purchase
        frequency; the spend axis is logarithmic.
      </p>
    </div>
  );
}
