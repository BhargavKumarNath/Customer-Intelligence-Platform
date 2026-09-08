"use client";

import { useMemo } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchPropensity, histogram, quantile, scoreDeciles } from "@/lib/client-data";
import { Histogram } from "@/components/charts/distribution";
import { ChartSkeleton, ErrorState } from "@/components/ui/states";
import { integer, percent } from "@/lib/format";

export function PropensityDistribution({ baseline }: { baseline: number }) {
  const query = useQuery({
    queryKey: ["propensity"],
    queryFn: ({ signal }) => fetchPropensity(signal),
  });

  const view = useMemo(() => {
    if (!query.data) return null;
    const scores = Object.values(query.data);
    const sorted = [...scores].sort((a, b) => a - b);
    const p95 = quantile(sorted, 0.95);
    const median = quantile(sorted, 0.5);
    const bins = histogram(scores, 40, 0, 1);
    const deciles = scoreDeciles(scores);
    return { count: scores.length, p95, median, bins, deciles };
  }, [query.data]);

  if (query.isPending) return <ChartSkeleton height={280} />;
  if (query.isError || !view) {
    return (
      <ErrorState title="Could not load propensity scores" onRetry={() => query.refetch()}>
        propensity.json is fetched only on this page and the user intelligence page.
      </ErrorState>
    );
  }

  return (
    <div className="space-y-6">
      <div>
        <Histogram
          bins={view.bins}
          xFormat={(v) => v.toFixed(1)}
          markers={[{ value: view.p95, label: "95th pct" }]}
          highlightFrom={view.p95}
          height={280}
        />
        <p className="mt-2 text-2xs text-ink-faint">
          {integer(view.count)} scored users. Median score {percent(view.median, 1)}. The shaded bars
          are the top five percent, above {percent(view.p95, 1)}.
        </p>
      </div>

      <div className="overflow-x-auto rounded-md border border-rule">
        <table className="w-full text-sm">
          <caption className="sr-only">Conversion implied by propensity decile</caption>
          <thead className="bg-surface-sunken text-2xs uppercase tracking-wider text-ink-faint">
            <tr>
              <th className="px-4 py-2 text-left font-medium">Decile</th>
              <th className="px-4 py-2 text-right font-medium">Users</th>
              <th className="px-4 py-2 text-right font-medium">Mean score</th>
              <th className="px-4 py-2 text-right font-medium">Score floor</th>
              <th className="px-4 py-2 text-right font-medium">Lift vs base</th>
            </tr>
          </thead>
          <tbody>
            {view.deciles.map((d) => (
              <tr key={d.decile} className="border-t border-rule/70">
                <td className="px-4 py-2 text-ink">
                  {d.decile === 1 ? "Top 10%" : d.decile === 10 ? "Bottom 10%" : `Decile ${d.decile}`}
                </td>
                <td className="tnum px-4 py-2 text-right text-ink-muted">{integer(d.users)}</td>
                <td className="tnum px-4 py-2 text-right text-ink-muted">{percent(d.meanScore, 1)}</td>
                <td className="tnum px-4 py-2 text-right text-ink-faint">{percent(d.minScore, 1)}</td>
                <td className="tnum px-4 py-2 text-right text-ink">
                  {baseline > 0 ? `${(d.meanScore / baseline).toFixed(1)}x` : "n/a"}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <p className="text-2xs text-ink-faint">
        Lift compares each decile&apos;s mean predicted probability to the {percent(baseline)}{" "}
        baseline conversion rate from the model card. It is an estimate of relative response, not a
        measured campaign result.
      </p>
    </div>
  );
}
