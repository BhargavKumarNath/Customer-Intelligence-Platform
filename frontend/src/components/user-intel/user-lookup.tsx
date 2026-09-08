"use client";

import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { fetchPropensity, fetchSegments } from "@/lib/client-data";
import { LoadingBlock, ErrorState, EmptyState } from "@/components/ui/states";
import { currency, integer, percent } from "@/lib/format";
import { cn } from "@/lib/cn";

export function UserLookup() {
  const [armed, setArmed] = useState(false);
  const [input, setInput] = useState("");
  const [userId, setUserId] = useState<string | null>(null);

  const segments = useQuery({
    queryKey: ["segments"],
    queryFn: ({ signal }) => fetchSegments(signal),
    enabled: armed,
  });
  const propensity = useQuery({
    queryKey: ["propensity"],
    queryFn: ({ signal }) => fetchPropensity(signal),
    enabled: armed,
  });

  const ready = segments.data && propensity.data;

  const examples = useMemo(() => {
    if (!segments.data || !propensity.data) return [];
    const values = Object.values(segments.data);
    const champion = values.find((u) => u.segment === "Champions");
    const atRisk = values.find((u) => u.segment === "At Risk");
    let topId = "";
    let topScore = -1;
    for (const [id, score] of Object.entries(propensity.data)) {
      if (score > topScore && segments.data[id]) {
        topScore = score;
        topId = id;
      }
    }
    return [
      champion && { id: String(champion.user_id), label: "a Champion" },
      atRisk && { id: String(atRisk.user_id), label: "an At Risk buyer" },
      topId && { id: topId, label: "the highest propensity" },
    ].filter(Boolean) as { id: string; label: string }[];
  }, [segments.data, propensity.data]);

  const sortedScores = useMemo(
    () => (propensity.data ? Object.values(propensity.data).sort((a, b) => a - b) : []),
    [propensity.data],
  );

  const result = useMemo(() => {
    if (!userId || !segments.data || !propensity.data) return null;
    const seg = segments.data[userId] ?? null;
    const score = propensity.data[userId];
    let percentile: number | null = null;
    if (score != null && sortedScores.length) {
      let lo = 0;
      let hi = sortedScores.length;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (sortedScores[mid]! < score) lo = mid + 1;
        else hi = mid;
      }
      percentile = lo / sortedScores.length;
    }
    return { seg, score: score ?? null, percentile };
  }, [userId, segments.data, propensity.data, sortedScores]);

  if (!armed) {
    return (
      <div className="panel p-6">
        <p className="text-sm text-ink-muted">
          The lookup needs two files that are not loaded on any other page: the full segment table
          and every propensity score. About 4 MB, fetched once and cached.
        </p>
        <button
          type="button"
          onClick={() => setArmed(true)}
          className="mt-4 rounded-md bg-accent px-3.5 py-2 text-sm font-medium text-[hsl(40_40%_98%)] transition-colors hover:bg-accent-ink"
        >
          Load user data
        </button>
      </div>
    );
  }

  if (segments.isError || propensity.isError) {
    return (
      <ErrorState
        title="User data did not load"
        onRetry={() => {
          segments.refetch();
          propensity.refetch();
        }}
      >
        One of segments.json or propensity.json failed to fetch.
      </ErrorState>
    );
  }

  if (!ready) return <LoadingBlock label="Loading segments and propensity scores" />;

  return (
    <div className="space-y-4">
      <form
        className="flex flex-wrap items-end gap-3"
        onSubmit={(e) => {
          e.preventDefault();
          setUserId(input.trim() || null);
        }}
      >
        <div className="flex-1 min-w-[200px]">
          <label htmlFor="uid" className="mb-1.5 block text-2xs uppercase tracking-wider text-ink-faint">
            User id
          </label>
          <input
            id="uid"
            inputMode="numeric"
            value={input}
            onChange={(e) => setInput(e.target.value)}
            placeholder="e.g. 512807853"
            className="tnum w-full rounded-md border border-rule bg-surface px-3 py-2 text-sm outline-none focus-visible:ring-2 focus-visible:ring-accent"
          />
        </div>
        <button
          type="submit"
          className="rounded-md border border-rule-strong px-3.5 py-2 text-sm text-ink transition-colors hover:bg-surface-sunken"
        >
          Look up
        </button>
      </form>

      {examples.length > 0 && (
        <div className="flex flex-wrap items-center gap-2 text-2xs text-ink-faint">
          <span>Try</span>
          {examples.map((ex) => (
            <button
              key={ex.id}
              type="button"
              onClick={() => {
                setInput(ex.id);
                setUserId(ex.id);
              }}
              className="tnum rounded-sm border border-rule px-1.5 py-0.5 text-ink-muted transition-colors hover:border-accent/40 hover:text-accent-ink"
            >
              {ex.id} · {ex.label}
            </button>
          ))}
        </div>
      )}

      {userId && !result?.seg && result?.score == null && (
        <EmptyState title="No record for that id">
          This user is not in the segment table and has no propensity score. Only buyers appear in
          segments; only pre-November users are scored.
        </EmptyState>
      )}

      {result && (result.seg || result.score != null) && (
        <div className="grid gap-4 sm:grid-cols-2">
          <div className="panel p-5">
            <span className="kicker">RFM segment</span>
            {result.seg ? (
              <>
                <p className="mt-1.5 font-display text-2xl text-ink">{result.seg.segment}</p>
                <dl className="mt-4 grid grid-cols-3 gap-3 text-sm">
                  <Metric label="Recency" value={`${integer(result.seg.recency_days)}d`} score={result.seg.r_score} />
                  <Metric label="Frequency" value={integer(result.seg.frequency)} score={result.seg.f_score} />
                  <Metric label="Monetary" value={currency(result.seg.monetary)} score={result.seg.m_score} />
                </dl>
              </>
            ) : (
              <p className="mt-2 text-sm text-ink-faint">Not a buyer, so not segmented.</p>
            )}
          </div>

          <div className="panel p-5">
            <span className="kicker">Purchase propensity</span>
            {result.score != null ? (
              <>
                <p className="tnum mt-1.5 text-3xl text-accent-ink">{percent(result.score, 1)}</p>
                <p className="mt-1 text-2xs text-ink-faint">
                  probability of a November purchase from October behavior
                </p>
                {result.percentile != null && (
                  <div className="mt-4">
                    <div className="flex justify-between text-2xs text-ink-faint">
                      <span>0</span>
                      <span>higher than {percent(result.percentile, 0)} of scored users</span>
                      <span>1</span>
                    </div>
                    <div className="mt-1 h-2 w-full rounded-full bg-surface-sunken">
                      <div
                        className="h-full rounded-full bg-accent"
                        style={{ width: `${Math.max(2, result.percentile * 100)}%` }}
                      />
                    </div>
                  </div>
                )}
              </>
            ) : (
              <p className="mt-2 text-sm text-ink-faint">No score. This user has no October activity.</p>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function Metric({ label, value, score }: { label: string; value: string; score: number }) {
  return (
    <div>
      <dt className="text-2xs uppercase tracking-wider text-ink-faint">{label}</dt>
      <dd className="tnum mt-0.5 text-ink">{value}</dd>
      <div className="mt-1 flex gap-0.5" aria-label={`${label} score ${score} of 5`}>
        {[1, 2, 3, 4, 5].map((n) => (
          <span
            key={n}
            className={cn("h-1 w-3 rounded-full", n <= score ? "bg-accent" : "bg-rule")}
          />
        ))}
      </div>
    </div>
  );
}
