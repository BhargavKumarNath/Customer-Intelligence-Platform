// RFM segment names, in display order. Must match RFM_SEGMENTS in
// src/processing/dimensional_model.py (the one rule every build uses).
// Kept out of palette.ts, which is a client module, so server pages can import it.

/** Stable colour for a named RFM segment, independent of array order. */
export const SEGMENT_ORDER = [
  "Champions",
  "Loyal Customers",
  "New Customers",
  "Promising",
  "Need Attention",
  "At Risk",
  "Cant Lose Them",
  "Hibernating",
];

export function segmentColorIndex(segment: string): number {
  const i = SEGMENT_ORDER.indexOf(segment);
  return i === -1 ? SEGMENT_ORDER.length - 1 : i;
}

/** Lapsed high-frequency buyers (R=1, F>=4): the reactivation-test population, as in
 * src/analysis/ab_testing.py. */
export const REACTIVATION_SEGMENT = "Cant Lose Them";

/** Default segment for a picker: the reactivation segment when present. */
export function defaultSegment(segments: string[]): string {
  return segments.includes(REACTIVATION_SEGMENT) ? REACTIVATION_SEGMENT : (segments[0] ?? "");
}
