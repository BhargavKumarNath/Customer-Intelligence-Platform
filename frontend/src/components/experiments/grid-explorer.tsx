"use client";

import { useMemo, useState } from "react";
import type { AbGridCell } from "@/lib/types";
import { DataTable, type Column } from "@/components/ui/data-table";
import { SegmentedControl } from "@/components/ui/controls";
import { Tag } from "@/components/ui/primitives";
import { percent, pValueLabel } from "@/lib/format";

export function GridExplorer({ grid, segments }: { grid: AbGridCell[]; segments: string[] }) {
  const [segment, setSegment] = useState(segments[0] ?? "At Risk");
  const rows = useMemo(
    () => grid.filter((c) => c.segment === segment).sort((a, b) => a.lift - b.lift || a.confidence_level - b.confidence_level),
    [grid, segment],
  );

  const columns: Column<AbGridCell>[] = [
    {
      key: "lift",
      header: "Design lift",
      numeric: true,
      emphasise: true,
      render: (r) => percent(r.lift, 0),
      sortValue: (r) => r.lift,
    },
    {
      key: "confidence_level",
      header: "Confidence",
      numeric: true,
      render: (r) => percent(r.confidence_level, 0),
      sortValue: (r) => r.confidence_level,
    },
    {
      key: "relative_lift",
      header: "Observed lift",
      numeric: true,
      render: (r) => percent(r.relative_lift, 1),
      sortValue: (r) => r.relative_lift,
    },
    {
      key: "p_value",
      header: "p-value",
      numeric: true,
      render: (r) => pValueLabel(r.p_value),
      sortValue: (r) => r.p_value,
    },
    {
      key: "power",
      header: "Power",
      numeric: true,
      render: (r) =>
        r.power_undefined || r.statistical_power == null ? (
          <span className="text-ink-faint">undefined</span>
        ) : (
          r.statistical_power.toFixed(3)
        ),
      sortValue: (r) => r.statistical_power ?? -1,
    },
    {
      key: "ci",
      header: "95% CI (abs)",
      numeric: true,
      render: (r) => `[${r.ci_95_lower.toFixed(4)}, ${r.ci_95_upper.toFixed(4)}]`,
    },
    {
      key: "is_significant",
      header: "Result",
      render: (r) => (
        <Tag tone={r.is_significant ? "positive" : "caution"}>{r.is_significant ? "sig" : "n.s."}</Tag>
      ),
      sortValue: (r) => (r.is_significant ? 1 : 0),
    },
  ];

  return (
    <div className="space-y-4">
      <SegmentedControl
        ariaLabel="Filter grid by segment"
        value={segment}
        onChange={setSegment}
        options={segments.map((s) => ({ value: s, label: s }))}
      />
      <DataTable
        columns={columns}
        rows={rows}
        rowKey={(r) => `${r.segment}-${r.lift}-${r.confidence_level}`}
        dense
        caption={`Precomputed A/B grid for the ${segment} segment`}
      />
      <p className="text-2xs leading-relaxed text-ink-faint">
        Each row is one run of the Python A/B engine at a fixed design lift and confidence level.
        Power is undefined where the solver saturates at the top of the lift range. The calculator
        above reproduces any of these rows from its stored counts.
      </p>
    </div>
  );
}
