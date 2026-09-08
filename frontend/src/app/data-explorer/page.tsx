import type { Metadata } from "next";
import { getManifest, getMeta } from "@/lib/data";
import { PageHeader, Section } from "@/components/ui/primitives";
import { SqlPlayground } from "@/components/explorer/sql-playground";
import { integer } from "@/lib/format";

export const metadata: Metadata = {
  title: "Data explorer",
  description:
    "Run SQL against the trimmed event log in the browser with DuckDB compiled to WebAssembly. No backend.",
};

export default function DataExplorerPage() {
  const meta = getMeta();
  const manifest = getManifest();
  const parquetBytes = manifest.files["events_trimmed.parquet"] ?? 0;

  return (
    <div className="space-y-12">
      <PageHeader
        index="05"
        kicker="Look closer"
        title="Data explorer"
        lede={
          <>
            The dashboards answer fixed questions. This is for the ones they do not. It loads the
            event log into an in-browser copy of DuckDB, so you can write your own SQL against{" "}
            {integer(meta.dataset_rows)} rows without anything leaving your machine.
          </>
        }
        meta={
          <>
            <span>{integer(meta.dataset_rows)} events</span>
            <span>{(parquetBytes / 1024 / 1024).toFixed(1)} MB download</span>
            <span>runs client-side</span>
          </>
        }
      />

      <Section
        index="01"
        title="SQL playground"
        description="Start the engine, pick a preset or write your own, and run. The events view re-derives event_time from the stored timestamp."
      >
        <SqlPlayground parquetBytes={parquetBytes} />
      </Section>

      <Section index="02" title="Notes">
        <div className="max-w-prose space-y-3 text-sm leading-relaxed text-ink-muted">
          <p>
            The file is the same trimmed Parquet the rest of the site is built from. The UUID session
            key is replaced with a dense integer and the timestamp with epoch seconds, both
            losslessly, to keep the download small.
          </p>
          <p>
            The WebAssembly runtime is fetched from a CDN on first use. If your network blocks it,
            this page will say so rather than hang.
          </p>
        </div>
      </Section>
    </div>
  );
}
