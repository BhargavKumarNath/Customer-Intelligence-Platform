"use client";

/**
 * In-browser OLAP for the SQL playground (/data-explorer). DuckDB-WASM loads the
 * trimmed events Parquet once, exposes it as a view that re-derives event_time,
 * and runs arbitrary read SQL with two guardrails: a single-statement rule with a
 * forced row cap, and a wall-clock timeout that tears the worker down rather than
 * letting a runaway query hang the tab.
 */

import type { AsyncDuckDB } from "@duckdb/duckdb-wasm";

export const PARQUET_URL = "/data/current/events_trimmed.parquet";
export const DEFAULT_ROW_CAP = 2000;
export const DEFAULT_TIMEOUT_MS = 8000;
export const INIT_TIMEOUT_MS = 40_000;

export class QueryTimeoutError extends Error {
  constructor(ms: number) {
    super(`Query cancelled after ${ms} ms. Narrow the query or add a tighter filter.`);
    this.name = "QueryTimeoutError";
  }
}

export class QueryShapeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "QueryShapeError";
  }
}

export interface QueryResult {
  columns: string[];
  rows: Array<Record<string, unknown>>;
  rowCount: number;
  truncated: boolean;
  elapsedMs: number;
}

const VIEW_SQL = `
  CREATE OR REPLACE VIEW events AS
  SELECT
    to_timestamp(ts)            AS event_time,
    event_type,
    product_id,
    price,
    user_id,
    session_id
  FROM read_parquet('events.parquet')
`;

function normaliseQuery(sql: string): string {
  const trimmed = sql.trim().replace(/;\s*$/, "");
  if (trimmed.includes(";")) {
    throw new QueryShapeError("Run one statement at a time. Remove the ';' between statements.");
  }
  if (!/^\s*(with|select|from|pragma|describe|explain|show|table|values)\b/i.test(trimmed)) {
    throw new QueryShapeError("Only read queries are allowed here (SELECT / WITH / DESCRIBE / SHOW).");
  }
  return trimmed;
}

export class ExplorerDB {
  private db: AsyncDuckDB | null = null;
  private worker: Worker | null = null;
  private workerUrl: string | null = null;
  private ready: Promise<void> | null = null;

  async init(onProgress?: (stage: string) => void): Promise<void> {
    if (this.ready) return this.ready;
    this.ready = Promise.race([
      this._init(onProgress),
      new Promise<never>((_, reject) =>
        setTimeout(
          () =>
            reject(
              new Error(
                `Engine did not start within ${Math.round(INIT_TIMEOUT_MS / 1000)} s. The WebAssembly runtime is fetched from a CDN on first use; a slow or blocked network stops it here.`,
              ),
            ),
          INIT_TIMEOUT_MS,
        ),
      ),
    ]).catch((err) => {
      this.ready = null;
      throw err;
    });
    return this.ready;
  }

  private async _init(onProgress?: (stage: string) => void): Promise<void> {
    // Import the browser entry directly: the package index also references a Node
    // build with a dynamic require that webpack flags as a critical dependency.
    const duckdb = await import("@duckdb/duckdb-wasm/dist/duckdb-browser");
    onProgress?.("Selecting runtime");
    const bundles = duckdb.getJsDelivrBundles();
    const bundle = await duckdb.selectBundle(bundles);

    // Cross-origin worker: wrap the CDN script in a same-origin blob.
    this.workerUrl = URL.createObjectURL(
      new Blob([`importScripts("${bundle.mainWorker}");`], { type: "text/javascript" }),
    );
    this.worker = new Worker(this.workerUrl);
    const logger = new duckdb.ConsoleLogger(duckdb.LogLevel.WARNING);
    this.db = new duckdb.AsyncDuckDB(logger, this.worker);

    onProgress?.("Loading engine");
    await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);

    onProgress?.("Fetching dataset");
    const res = await fetch(PARQUET_URL, { cache: "force-cache" });
    if (!res.ok) throw new Error(`Could not fetch the events file (${res.status}).`);
    const buffer = new Uint8Array(await res.arrayBuffer());
    await this.db.registerFileBuffer("events.parquet", buffer);

    onProgress?.("Preparing view");
    const conn = await this.db.connect();
    try {
      await conn.query(VIEW_SQL);
    } finally {
      await conn.close();
    }
    onProgress?.("Ready");
  }

  async run(
    sql: string,
    opts: { rowCap?: number; timeoutMs?: number } = {},
  ): Promise<QueryResult> {
    if (!this.db) throw new Error("Engine is not initialised.");
    const rowCap = opts.rowCap ?? DEFAULT_ROW_CAP;
    const timeoutMs = opts.timeoutMs ?? DEFAULT_TIMEOUT_MS;
    const inner = normaliseQuery(sql);
    const wrapped = `SELECT * FROM (\n${inner}\n) AS _playground LIMIT ${rowCap + 1}`;

    const conn = await this.db.connect();
    const started = performance.now();
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      const table = await Promise.race([
        conn.query(wrapped),
        new Promise<never>((_, reject) => {
          timer = setTimeout(() => reject(new QueryTimeoutError(timeoutMs)), timeoutMs);
        }),
      ]);
      const columns = table.schema.fields.map((f: { name: string }) => f.name);
      const all = table.toArray().map((r: { toJSON: () => Record<string, unknown> }) => coerce(r.toJSON()));
      const truncated = all.length > rowCap;
      const rows = truncated ? all.slice(0, rowCap) : all;
      return {
        columns,
        rows,
        rowCount: rows.length,
        truncated,
        elapsedMs: performance.now() - started,
      };
    } catch (err) {
      if (err instanceof QueryTimeoutError) {
        // The worker is still chewing on the cancelled query; drop it so the next
        // run starts from a clean engine.
        await this.dispose();
      }
      throw err;
    } finally {
      if (timer) clearTimeout(timer);
      try {
        await conn.close();
      } catch {
        /* connection already torn down on timeout */
      }
    }
  }

  async dispose(): Promise<void> {
    try {
      await this.db?.terminate();
    } catch {
      /* ignore */
    }
    this.worker?.terminate();
    if (this.workerUrl) URL.revokeObjectURL(this.workerUrl);
    this.db = null;
    this.worker = null;
    this.workerUrl = null;
    this.ready = null;
  }
}

/** Arrow returns BigInt for 64-bit ints and Date-like values; make them JSON-safe. */
function coerce(row: Record<string, unknown>): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(row)) {
    if (typeof v === "bigint") out[k] = Number(v);
    else if (v instanceof Date) out[k] = v.toISOString().slice(0, 19).replace("T", " ");
    else out[k] = v;
  }
  return out;
}

export interface PresetQuery {
  id: string;
  label: string;
  description: string;
  sql: string;
}

export const PRESET_QUERIES: PresetQuery[] = [
  {
    id: "event-mix",
    label: "Event mix",
    description: "Share of views, carts, and purchases across the whole log.",
    sql: `SELECT event_type,
       count(*)                              AS events,
       round(100.0 * count(*) / sum(count(*)) OVER (), 2) AS pct
FROM events
GROUP BY event_type
ORDER BY events DESC`,
  },
  {
    id: "daily-revenue",
    label: "Revenue by day",
    description: "Purchase-event revenue per calendar day.",
    sql: `SELECT event_time::DATE AS day,
       count(*) FILTER (WHERE event_type = 'purchase')   AS orders,
       round(sum(price) FILTER (WHERE event_type = 'purchase'), 2) AS revenue
FROM events
GROUP BY day
ORDER BY day`,
  },
  {
    id: "price-bands",
    label: "Price bands",
    description: "How events distribute across price ranges.",
    sql: `SELECT CASE
         WHEN price < 10  THEN '1. under $10'
         WHEN price < 50  THEN '2. $10 to $50'
         WHEN price < 150 THEN '3. $50 to $150'
         WHEN price < 500 THEN '4. $150 to $500'
         ELSE '5. $500 and up'
       END AS band,
       count(*) AS events
FROM events
WHERE price > 0
GROUP BY band
ORDER BY band`,
  },
  {
    id: "top-products",
    label: "Most purchased products",
    description: "Product ids with the most purchase events.",
    sql: `SELECT product_id,
       count(*) FILTER (WHERE event_type = 'purchase') AS orders,
       round(avg(price), 2)                            AS avg_price
FROM events
GROUP BY product_id
HAVING orders > 0
ORDER BY orders DESC
LIMIT 25`,
  },
  {
    id: "session-depth",
    label: "Session depth",
    description: "Distribution of events per session (capped view).",
    sql: `WITH per_session AS (
  SELECT session_id, count(*) AS n
  FROM events
  GROUP BY session_id
)
SELECT least(n, 20) AS events_in_session,
       count(*)     AS sessions
FROM per_session
GROUP BY events_in_session
ORDER BY events_in_session`,
  },
];
