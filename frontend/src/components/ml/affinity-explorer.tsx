"use client";

import { useMemo, useState } from "react";
import type { AffinityFile } from "@/lib/types";
import { Select } from "@/components/ui/controls";
import { EmptyState } from "@/components/ui/states";
import { integer, multiple, percent } from "@/lib/format";

export function AffinityExplorer({ affinity }: { affinity: AffinityFile }) {
  const productIds = useMemo(
    () =>
      Object.keys(affinity.by_product).sort(
        (a, b) => (affinity.by_product[b]?.[0]?.lift ?? 0) - (affinity.by_product[a]?.[0]?.lift ?? 0),
      ),
    [affinity],
  );
  const [selected, setSelected] = useState(productIds[0] ?? "");
  const recs = affinity.by_product[selected] ?? [];

  return (
    <div className="space-y-5">
      <div className="flex flex-wrap items-end gap-4">
        <Select
          label="Anchor product"
          value={selected}
          onChange={setSelected}
          options={productIds.map((id) => ({
            value: id,
            label: `Product ${id}  (lift ${multiple(affinity.by_product[id]?.[0]?.lift ?? 0)})`,
          }))}
          className="min-w-[280px]"
        />
        <p className="text-2xs text-ink-faint">
          {productIds.length} products carry a rule. Pairs need at least three co-purchases and lift
          above 1.2.
        </p>
      </div>

      {recs.length === 0 ? (
        <EmptyState title="No co-purchase rule for this product" />
      ) : (
        <ul className="grid gap-3 sm:grid-cols-2">
          {recs.map((r) => (
            <li key={r.product_id} className="panel p-4">
              <div className="flex items-baseline justify-between">
                <span className="tnum text-sm text-ink">Product {r.product_id}</span>
                <span className="tnum text-lg text-accent-ink">{multiple(r.lift)}</span>
              </div>
              <dl className="mt-3 grid grid-cols-2 gap-2 text-2xs text-ink-faint">
                <div>
                  <dt>Confidence</dt>
                  <dd className="tnum mt-0.5 text-ink-muted">{percent(r.confidence, 1)}</dd>
                </div>
                <div>
                  <dt>Co-purchases</dt>
                  <dd className="tnum mt-0.5 text-ink-muted">{integer(r.pair_count)}</dd>
                </div>
              </dl>
              <p className="mt-3 text-2xs leading-relaxed text-ink-faint">
                A buyer of the anchor is {multiple(r.lift)} as likely to also buy this as a random
                buyer.
              </p>
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
