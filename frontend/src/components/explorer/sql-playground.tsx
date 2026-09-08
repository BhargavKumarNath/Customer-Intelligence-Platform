"use client";

import { useEffect, useRef, useState } from "react";
import {
  DEFAULT_TIMEOUT_MS,
  ExplorerDB,
  PRESET_QUERIES,
  QueryShapeError,
  QueryTimeoutError,
  type QueryResult,
} from "@/lib/duckdb";
import { Panel, Tag } from "@/components/ui/primitives";
import { SegmentedControl } from "@/components/ui/controls";
import { EmptyState, ErrorState, LoadingBlock } from "@/components/ui/states";
import { PlayIcon, SpinnerIcon } from "@/components/icons";
import { integer } from "@/lib/format";

const SCHEMA = [
  ["event_time", "TIMESTAMP", "derived from the stored epoch seconds"],
  ["event_type", "VARCHAR", "view, cart, remove_from_cart, or purchase"],
  ["product_id", "INTEGER", "catalogue id"],
  ["price", "DOUBLE", "unit price at event time"],
  ["user_id", "INTEGER", "pseudonymous user id"],
  ["session_id", "INTEGER", "dense id for one visit"],
];

type Status = "idle" | "starting" | "ready" | "error";

export function SqlPlayground({ parquetBytes }: { parquetBytes: number }) {
  const dbRef = useRef<ExplorerDB | null>(null);
  const [status, setStatus] = useState<Status>("idle");
  const [stage, setStage] = useState("");
  const [initError, setInitError] = useState<string | null>(null);

  const [sql, setSql] = useState(PRESET_QUERIES[0]!.sql);
  const [rowCap, setRowCap] = useState("2000");
  const [running, setRunning] = useState(false);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [queryError, setQueryError] = useState<string | null>(null);

  useEffect(() => {
    return () => {
      dbRef.current?.dispose();
      dbRef.current = null;
    };
  }, []);

  async function start() {
    setStatus("starting");
    setInitError(null);
    const db = new ExplorerDB();
    dbRef.current = db;
    try {
      await db.init((s) => setStage(s));
      setStatus("ready");
    } catch (err) {
      setInitError(err instanceof Error ? err.message : String(err));
      setStatus("error");
      await db.dispose();
      dbRef.current = null;
    }
  }

  async function run() {
    if (!dbRef.current || running) return;
    setRunning(true);
    setQueryError(null);
    try {
      const res = await dbRef.current.run(sql, { rowCap: Number(rowCap), timeoutMs: DEFAULT_TIMEOUT_MS });
      setResult(res);
    } catch (err) {
      setResult(null);
      if (err instanceof QueryTimeoutError) {
        setQueryError(`${err.message} The engine was reset, run again to reconnect.`);
        setStatus("idle");
      } else if (err instanceof QueryShapeError) {
        setQueryError(err.message);
      } else {
        setQueryError(err instanceof Error ? err.message : String(err));
      }
    } finally {
      setRunning(false);
    }
  }

  if (status === "idle" || status === "starting" || status === "error") {
    return (
      <Panel>
        <div className="max-w-prose">
          <p className="text-sm text-ink-muted">
            This runs DuckDB compiled to WebAssembly in your browser. It downloads the trimmed event
            log once ({(parquetBytes / 1024 / 1024).toFixed(1)} MB), then every query runs locally.
            Nothing is sent to a server.
          </p>
          {status === "error" && initError && (
            <div className="mt-4">
              <ErrorState title="The engine did not start" onRetry={start}>
                {initError} If your network blocks the CDN that hosts the WebAssembly runtime, this
                page cannot load here.
              </ErrorState>
            </div>
          )}
          {status === "starting" ? (
            <div className="mt-4">
              <LoadingBlock label={stage || "Starting"} />
            </div>
          ) : (
            <button
              type="button"
              onClick={start}
              className="mt-4 inline-flex items-center gap-2 rounded-md bg-accent px-3.5 py-2 text-sm font-medium text-[hsl(40_40%_98%)] transition-colors hover:bg-accent-ink"
            >
              Start the engine
            </button>
          )}
        </div>
      </Panel>
    );
  }

  return (
    <div className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_16rem]">
      <div className="space-y-4">
        <div className="flex flex-wrap gap-1.5">
          {PRESET_QUERIES.map((q) => (
            <button
              key={q.id}
              type="button"
              onClick={() => setSql(q.sql)}
              title={q.description}
              className="rounded-sm border border-rule px-2 py-1 text-2xs text-ink-muted transition-colors hover:border-accent/40 hover:text-accent-ink"
            >
              {q.label}
            </button>
          ))}
        </div>

        <div className="panel overflow-hidden">
          <textarea
            value={sql}
            onChange={(e) => setSql(e.target.value)}
            spellCheck={false}
            rows={9}
            className="tnum w-full resize-y bg-surface p-4 text-[13px] leading-relaxed text-ink outline-none"
            aria-label="SQL query"
          />
          <div className="flex flex-wrap items-center justify-between gap-3 border-t border-rule px-4 py-2.5">
            <SegmentedControl
              ariaLabel="Row cap"
              value={rowCap}
              onChange={setRowCap}
              options={[
                { value: "500", label: "500 rows" },
                { value: "2000", label: "2k rows" },
                { value: "5000", label: "5k rows" },
              ]}
            />
            <button
              type="button"
              onClick={run}
              disabled={running}
              className="inline-flex items-center gap-1.5 rounded-md bg-accent px-3 py-1.5 text-sm font-medium text-[hsl(40_40%_98%)] transition-colors hover:bg-accent-ink disabled:opacity-60"
            >
              {running ? <SpinnerIcon size={14} /> : <PlayIcon size={13} />}
              {running ? "Running" : "Run"}
            </button>
          </div>
        </div>

        {queryError && <ErrorState title="Query error">{queryError}</ErrorState>}

        {result && !queryError && (
          <div className="space-y-2">
            <div className="flex flex-wrap items-center gap-2 text-2xs text-ink-faint">
              <Tag>{integer(result.rowCount)} rows</Tag>
              <Tag>{result.elapsedMs.toFixed(0)} ms</Tag>
              {result.truncated && <Tag tone="caution">capped at {rowCap}</Tag>}
            </div>
            {result.rowCount === 0 ? (
              <EmptyState title="No rows returned" />
            ) : (
              <div className="max-h-[28rem] overflow-auto rounded-md border border-rule">
                <table className="w-full border-collapse text-[13px]">
                  <thead className="sticky top-0 bg-surface-sunken">
                    <tr>
                      {result.columns.map((c) => (
                        <th
                          key={c}
                          className="whitespace-nowrap border-b border-rule px-3 py-1.5 text-left text-2xs font-medium uppercase tracking-wider text-ink-faint"
                        >
                          {c}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {result.rows.map((row, i) => (
                      <tr key={i} className="hover:bg-surface-sunken/60">
                        {result.columns.map((c) => (
                          <td key={c} className="tnum whitespace-nowrap border-b border-rule/60 px-3 py-1.5 text-ink-muted">
                            {formatCell(row[c])}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </div>

      <aside className="space-y-4">
        <Panel title="events view">
          <ul className="space-y-2 text-xs">
            {SCHEMA.map(([col, type, note]) => (
              <li key={col}>
                <div className="flex items-baseline justify-between gap-2">
                  <code className="text-ink">{col}</code>
                  <span className="text-2xs text-ink-faint">{type}</span>
                </div>
                <p className="mt-0.5 text-2xs text-ink-faint">{note}</p>
              </li>
            ))}
          </ul>
        </Panel>
        <Panel title="Guardrails">
          <ul className="space-y-1.5 text-2xs leading-relaxed text-ink-faint">
            <li>One statement per run. No semicolons.</li>
            <li>Reads only: SELECT, WITH, DESCRIBE, SHOW.</li>
            <li>Results are capped at the row limit you pick.</li>
            <li>A query over {Math.round(DEFAULT_TIMEOUT_MS / 1000)} seconds is cancelled.</li>
          </ul>
        </Panel>
      </aside>
    </div>
  );
}

function formatCell(v: unknown): string {
  if (v == null) return "null";
  if (typeof v === "number") {
    return Number.isInteger(v) ? String(v) : v.toFixed(4);
  }
  return String(v);
}
