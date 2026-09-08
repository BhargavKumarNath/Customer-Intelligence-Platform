"use client";

import { useMemo, useState } from "react";
import { cn } from "@/lib/cn";
import { ChevronDownIcon } from "@/components/icons";

export interface Column<Row> {
  key: string;
  header: string;
  align?: "left" | "right";
  numeric?: boolean;
  render: (row: Row) => React.ReactNode;
  sortValue?: (row: Row) => number | string;
  emphasise?: boolean;
}

export function DataTable<Row>({
  columns,
  rows,
  rowKey,
  initialSort,
  dense = false,
  maxHeight,
  caption,
}: {
  columns: Column<Row>[];
  rows: Row[];
  rowKey: (row: Row, i: number) => string;
  initialSort?: { key: string; dir: "asc" | "desc" };
  dense?: boolean;
  maxHeight?: number;
  caption?: string;
}) {
  const [sort, setSort] = useState(initialSort ?? null);

  const sorted = useMemo(() => {
    if (!sort) return rows;
    const col = columns.find((c) => c.key === sort.key);
    if (!col?.sortValue) return rows;
    const dir = sort.dir === "asc" ? 1 : -1;
    return [...rows].sort((a, b) => {
      const av = col.sortValue!(a);
      const bv = col.sortValue!(b);
      if (av < bv) return -1 * dir;
      if (av > bv) return 1 * dir;
      return 0;
    });
  }, [rows, sort, columns]);

  return (
    <div
      className="overflow-auto rounded-md border border-rule"
      style={maxHeight ? { maxHeight } : undefined}
    >
      <table className="w-full border-collapse text-sm">
        {caption && <caption className="sr-only">{caption}</caption>}
        <thead className="sticky top-0 z-10 bg-surface-sunken">
          <tr>
            {columns.map((col) => {
              const sortable = Boolean(col.sortValue);
              const activeSort = sort?.key === col.key;
              return (
                <th
                  key={col.key}
                  scope="col"
                  className={cn(
                    "whitespace-nowrap border-b border-rule px-3 py-2 text-2xs font-medium uppercase tracking-wider text-ink-faint",
                    col.align === "right" || col.numeric ? "text-right" : "text-left",
                  )}
                  aria-sort={activeSort ? (sort!.dir === "asc" ? "ascending" : "descending") : "none"}
                >
                  {sortable ? (
                    <button
                      type="button"
                      onClick={() =>
                        setSort((s) =>
                          s?.key === col.key
                            ? { key: col.key, dir: s.dir === "asc" ? "desc" : "asc" }
                            : { key: col.key, dir: "desc" },
                        )
                      }
                      className={cn(
                        "inline-flex items-center gap-1 transition-colors hover:text-ink",
                        activeSort && "text-ink",
                      )}
                    >
                      {col.header}
                      <ChevronDownIcon
                        size={12}
                        className={cn(
                          "transition-transform",
                          activeSort && sort!.dir === "asc" && "rotate-180",
                          !activeSort && "opacity-30",
                        )}
                      />
                    </button>
                  ) : (
                    col.header
                  )}
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {sorted.map((row, i) => (
            <tr key={rowKey(row, i)} className="transition-colors hover:bg-surface-sunken/60">
              {columns.map((col) => (
                <td
                  key={col.key}
                  className={cn(
                    "border-b border-rule/70 px-3",
                    dense ? "py-1.5" : "py-2.5",
                    col.align === "right" || col.numeric ? "text-right" : "text-left",
                    col.numeric && "tnum",
                    col.emphasise ? "text-ink" : "text-ink-muted",
                  )}
                >
                  {col.render(row)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
