/**
 * Ported from app/components/glossary.py. The "inThisProject" lines are rewritten
 * to describe the sample build this site renders, not the full-dataset local run.
 */

export interface GlossaryEntry {
  term: string;
  full: string;
  definition: string;
  inThisProject: string;
  group: "Data engineering" | "Segmentation" | "Machine learning" | "Experimentation";
}

export const GLOSSARY: GlossaryEntry[] = [
  {
    term: "DuckDB",
    full: "DuckDB analytics database",
    definition:
      "An embedded column-store built for analytical queries. It runs in-process with no server, the way SQLite does for transactional work.",
    inThisProject:
      "The build pipeline uses it to turn the raw event log into a star schema. The data explorer runs the same engine compiled to WebAssembly, directly in your browser.",
    group: "Data engineering",
  },
  {
    term: "Star schema",
    full: "Dimensional model",
    definition:
      "A warehouse layout that splits data into fact tables (measurements, one row per event or per day) and dimension tables (attributes of a user or product). Queries join a fact to a few dimensions.",
    inThisProject:
      "dim_users and dim_products hold attributes; fact_sessions and fact_daily_kpis hold the measurements. Row counts are on the method page.",
    group: "Data engineering",
  },
  {
    term: "Sessionization",
    full: "Session construction",
    definition:
      "Grouping a user's sequential events into visits. Here a visit is one server-assigned session id.",
    inThisProject:
      "The event log collapses into fact_sessions, one row per visit, carrying duration, event count, and whether the visit ended in a purchase.",
    group: "Data engineering",
  },
  {
    term: "RFM",
    full: "Recency, frequency, monetary",
    definition:
      "A segmentation that scores each buyer on how recently they purchased, how often, and how much they have spent, then names groups from the score pattern.",
    inThisProject:
      "Each metric is bucketed into fifths with NTILE(5). Recency is inverted so a recent buyer scores high. Segment names come from the recency and frequency scores; spend is reported per segment but does not drive the name.",
    group: "Segmentation",
  },
  {
    term: "NTILE",
    full: "N-tile window function",
    definition:
      "A SQL function that splits ordered rows into N equal-sized buckets. NTILE(5) produces quintiles.",
    inThisProject: "Used to turn raw recency, frequency, and spend into 1 to 5 scores for RFM.",
    group: "Segmentation",
  },
  {
    term: "Cohort retention",
    full: "Cohort retention analysis",
    definition:
      "Group users by the week they first appeared, then track what share is still active in each later week. It separates a discovery problem from a lifecycle problem.",
    inThisProject:
      "The retention grid on the user intelligence page is built from weekly_retention. Week 0 is always 100 percent by construction.",
    group: "Segmentation",
  },
  {
    term: "Propensity model",
    full: "Purchase propensity",
    definition:
      "A model that estimates the probability a user buys in the next period from their past behavior.",
    inThisProject:
      "A LightGBM classifier trained on a temporal split: October behavior predicts a November purchase. Its card and drivers are on the propensity page.",
    group: "Machine learning",
  },
  {
    term: "LightGBM",
    full: "Light gradient boosting machine",
    definition:
      "A gradient-boosted decision tree library. It grows trees leaf-wise and handles class imbalance well, which suits a low base-rate target like purchase.",
    inThisProject:
      "The frozen model is a raw Booster loaded from LightGBM's native text format. Predictions are identical whether it loads from that or the legacy pickle.",
    group: "Machine learning",
  },
  {
    term: "AUC-ROC",
    full: "Area under the ROC curve",
    definition:
      "The probability that the model ranks a random buyer above a random non-buyer. 0.5 is a coin flip, 1.0 is perfect ranking.",
    inThisProject: "Reported on held-out users on the propensity page, straight from metrics.json.",
    group: "Machine learning",
  },
  {
    term: "Lift (targeting)",
    full: "Top-percentile lift",
    definition:
      "The conversion rate of a top slice of the ranked population divided by the overall rate. A lift of 3 means that slice converts three times as often as average.",
    inThisProject: "Measured on the top five percent of users by predicted propensity.",
    group: "Machine learning",
  },
  {
    term: "Market basket lift",
    full: "Association rule lift",
    definition:
      "For a pair of products, the chance they are bought together divided by what you would expect if the two were independent. Above 1 is a real affinity.",
    inThisProject:
      "Product pairs are mined from co-purchases in the same session. The affinity table keeps pairs with at least three co-occurrences and lift above 1.2.",
    group: "Experimentation",
  },
  {
    term: "Welch's t-test",
    full: "Unequal-variance t-test",
    definition:
      "A two-sample test of whether two means differ that does not assume the groups have the same variance. Standard for A/B conversion comparisons.",
    inThisProject:
      "The experiment calculator runs it on the two arms' conversion outcomes, matching the Python engine within rounding.",
    group: "Experimentation",
  },
  {
    term: "Statistical power",
    full: "Power (1 minus type II error)",
    definition:
      "The chance a test detects a real effect of a given size. Teams usually aim for 0.8 before running an experiment.",
    inThisProject:
      "Computed post-hoc for each calculator run and precomputed across a segment by lift by confidence grid.",
    group: "Experimentation",
  },
];

export interface StackLayer {
  layer: string;
  choice: string;
  reason: string;
}

export const STACK: StackLayer[] = [
  {
    layer: "Query engine",
    choice: "DuckDB",
    reason: "In-process column store. Runs the pipeline offline and the explorer in the browser from the same SQL.",
  },
  {
    layer: "Pipeline",
    choice: "Python, DuckDB SQL, LightGBM",
    reason: "SQL for the joins and aggregations, LightGBM for the purchase model, one script to produce every artifact.",
  },
  {
    layer: "Artifacts",
    choice: "Versioned JSON and Parquet",
    reason: "Byte-deterministic output keyed by commit. The frontend build pins one set and fails if it is missing.",
  },
  {
    layer: "Frontend",
    choice: "Next.js App Router, static export",
    reason: "Prerendered HTML on a CDN. No server on the request path, so no cold start.",
  },
  {
    layer: "Charts",
    choice: "Recharts and hand-built SVG",
    reason: "Recharts for standard series, custom SVG for the cohort grid and funnel where a library would fight the design.",
  },
  {
    layer: "In-browser SQL",
    choice: "DuckDB-WASM",
    reason: "Ad-hoc queries over the trimmed event log without shipping the data through a backend.",
  },
];
