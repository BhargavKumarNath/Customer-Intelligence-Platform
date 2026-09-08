/**
 * Shapes of the Phase 2 static artifacts (scripts/build_static_artifacts.py).
 * These mirror src/domain/artifacts.py and src/domain/models.py. When the Phase 3
 * API is generated to an openapi.json, this file is the hand-written stand-in for
 * openapi-typescript output until that pipeline exists (deployment_stages.md P4-2).
 */

export interface ArtifactMeta {
  git_sha: string;
  built_at: string;
  date_range: [string, string];
  dataset_rows: number;
  users: number;
  row_counts: Record<string, number>;
  segments: string[];
  propensity_users: number;
}

export interface ModelMetrics {
  trained_at: string;
  git_sha: string;
  train_rows: number;
  test_rows: number;
  best_iteration: number;
  auc_roc: number;
  precision_top5pct: number;
  recall_top5pct: number;
  baseline_conversion_rate: number;
  top5pct_conversion_rate: number;
  lift_top5pct: number;
  feature_importance_gain: Record<string, number>;
  params: Record<string, string | number | boolean>;
}

export interface DailyKpi {
  date: string;
  daily_events: number;
  dau: number;
  daily_sessions: number;
  views: number;
  carts: number;
  purchases: number;
  daily_revenue: number;
}

export interface WeeklyRetentionRow {
  cohort_week: string;
  cohort_size: number;
  weeks_since_first: number;
  active_users: number;
  retention_rate: number;
}

export interface SegmentSummary {
  segment: string;
  population: number;
  pct_of_buyers: number;
  avg_recency_days: number;
  avg_frequency: number;
  avg_monetary: number;
  total_monetary: number;
  avg_rfm_total: number;
}

export interface UserSegment {
  user_id: number;
  recency_days: number;
  frequency: number;
  monetary: number;
  r_score: number;
  f_score: number;
  m_score: number;
  rfm_total: number;
  segment: string;
}

export interface ProductRecommendation {
  product_id: number;
  pair_count: number;
  confidence: number;
  lift: number;
}

export interface AffinityPair {
  product_a: number;
  product_b: number;
  pair_count: number;
  confidence: number;
  lift: number;
}

export interface AffinityFile {
  pairs: AffinityPair[];
  by_product: Record<string, ProductRecommendation[]>;
}

export interface AbGridCell {
  segment: string;
  lift: number;
  confidence_level: number;
  control_visitors: number;
  treatment_visitors: number;
  control_conversion_rate: number;
  treatment_conversion_rate: number;
  relative_lift: number;
  p_value: number;
  is_significant: boolean;
  ci_95_lower: number;
  ci_95_upper: number;
  statistical_power: number | null;
  power_undefined: boolean;
}

export interface DataManifest {
  sha: string;
  /** How sync-data.mjs resolved the pin: DATA_SHA env, data.lock, or the sole dist dir. */
  pinSource: "env" | "lock" | "dist";
  gitSha: string;
  builtAt: string;
  syncedAt: string;
  datasetRows: number;
  files: Record<string, number>;
}

/** segments.json is { [user_id]: UserSegment }; propensity.json is { [user_id]: number }. */
export type SegmentsFile = Record<string, UserSegment>;
export type PropensityFile = Record<string, number>;
