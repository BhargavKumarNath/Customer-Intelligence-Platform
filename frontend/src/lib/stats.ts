/**
 * A TypeScript port of src/analysis/ab_testing.py :: ABTestEngine.analyze_experiment.
 *
 * The Python engine, given a two-arm experiment of Bernoulli outcomes, reports:
 *   - relative lift              (t_rate - c_rate) / c_rate
 *   - Welch's t-test p-value     scipy.stats.ttest_ind(..., equal_var=False)
 *   - a delta-method 95% CI      diff +/- z_(1-a/2) * sqrt(SE_c^2 + SE_t^2)
 *   - post-hoc power             statsmodels TTestIndPower.solve_power(ratio=1)
 *
 * Welch's t-test on 0/1 arrays depends only on the per-arm counts (n, k), so this
 * port takes summary counts and reproduces the Python output bit-for-bit on the
 * deterministic statistics. The RNG that the Python side uses to *generate* a
 * synthetic experiment is deliberately not reproduced here (see
 * deployment_stages.md Phase 5): only analyze() is under parity test.
 *
 * Special functions (log-gamma, regularized incomplete beta, normal + Student-t
 * quantiles, noncentral-t CDF via Algorithm AS 243 / Lenth 1989) are implemented
 * inline so the page ships no stats dependency.
 */

const LN_SQRT_2PI = 0.9189385332046727; // 0.5 * ln(2*pi)
const SQRT_2_OVER_PI = 0.7978845608028654; // sqrt(2/pi)
const LN_SQRT_PI = 0.5723649429247001; // ln(sqrt(pi))

// --------------------------------------------------------------------------- //
// log-gamma (Lanczos, g = 7)
// --------------------------------------------------------------------------- //
const LANCZOS = [
  0.99999999999980993, 676.5203681218851, -1259.1392167224028, 771.32342877765313,
  -176.61502916214059, 12.507343278686905, -0.13857109526572012, 9.9843695780195716e-6,
  1.5056327351493116e-7,
];

export function logGamma(x: number): number {
  if (x < 0.5) {
    return Math.log(Math.PI / Math.sin(Math.PI * x)) - logGamma(1 - x);
  }
  const z = x - 1;
  let a = LANCZOS[0]!;
  const t = z + 7 + 0.5;
  for (let i = 1; i < 9; i += 1) a += LANCZOS[i]! / (z + i);
  return LN_SQRT_2PI + (z + 0.5) * Math.log(t) - t + Math.log(a);
}

// --------------------------------------------------------------------------- //
// erfc / normal CDF (Numerical Recipes Chebyshev erfccheb, error < 1e-15)
// --------------------------------------------------------------------------- //
const ERFC_COF = [
  -1.3026537197817094, 6.4196979235649026e-1, 1.9476473204185836e-2, -9.561514786808631e-3,
  -9.46595344482036e-4, 3.66839497852761e-4, 4.2523324806907e-5, -2.0278578112534e-5,
  -1.624290004647e-6, 1.30365583558e-6, 1.5626441722e-8, -8.5238095915e-8, 6.529054439e-9,
  5.059343495e-9, -9.91364156e-10, -2.27365122e-10, 9.6467911e-11, 2.394038e-12, -6.886027e-12,
  8.94487e-13, 3.13092e-13, -1.12708e-13, 3.81e-16, 7.106e-15,
];

function erfc(x: number): number {
  const z = Math.abs(x);
  const t = 2 / (2 + z);
  const ty = 4 * t - 2;
  let d = 0;
  let dd = 0;
  for (let j = ERFC_COF.length - 1; j > 0; j -= 1) {
    const tmp = d;
    d = ty * d - dd + ERFC_COF[j]!;
    dd = tmp;
  }
  const res = t * Math.exp(-z * z + 0.5 * (ERFC_COF[0]! + ty * d) - dd);
  return x >= 0 ? res : 2 - res;
}

export function normalCdf(x: number): number {
  return 1 - 0.5 * erfc(x / Math.SQRT2);
}

// Acklam's inverse normal CDF + one Halley step against the high-accuracy erfc.
const A = [
  -3.969683028665376e1, 2.209460984245205e2, -2.759285104469687e2, 1.38357751867269e2,
  -3.066479806614716e1, 2.506628277459239,
];
const B = [
  -5.447609879822406e1, 1.615858368580409e2, -1.556989798598866e2, 6.680131188771972e1,
  -1.328068155288572e1,
];
const C = [
  -7.784894002430293e-3, -3.223964580411365e-1, -2.400758277161838, -2.549732539343734,
  4.374664141464968, 2.938163982698783,
];
const D = [
  7.784695709041462e-3, 3.224671290700398e-1, 2.445134137142996, 3.754408661907416,
];

export function normalInv(p: number): number {
  if (p <= 0) return -Infinity;
  if (p >= 1) return Infinity;
  const pLow = 0.02425;
  let x: number;
  if (p < pLow) {
    const q = Math.sqrt(-2 * Math.log(p));
    x =
      (((((C[0]! * q + C[1]!) * q + C[2]!) * q + C[3]!) * q + C[4]!) * q + C[5]!) /
      ((((D[0]! * q + D[1]!) * q + D[2]!) * q + D[3]!) * q + 1);
  } else if (p <= 1 - pLow) {
    const q = p - 0.5;
    const r = q * q;
    x =
      ((((((A[0]! * r + A[1]!) * r + A[2]!) * r + A[3]!) * r + A[4]!) * r + A[5]!) * q) /
      (((((B[0]! * r + B[1]!) * r + B[2]!) * r + B[3]!) * r + B[4]!) * r + 1);
  } else {
    const q = Math.sqrt(-2 * Math.log(1 - p));
    x = -(
      (((((C[0]! * q + C[1]!) * q + C[2]!) * q + C[3]!) * q + C[4]!) * q + C[5]!) /
      ((((D[0]! * q + D[1]!) * q + D[2]!) * q + D[3]!) * q + 1)
    );
  }
  // Halley refinement
  const e = normalCdf(x) - p;
  const u = e * Math.sqrt(2 * Math.PI) * Math.exp((x * x) / 2);
  x = x - u / (1 + (x * u) / 2);
  return x;
}

// --------------------------------------------------------------------------- //
// regularized incomplete beta I_x(a, b) (Numerical Recipes continued fraction)
// --------------------------------------------------------------------------- //
function betacf(x: number, a: number, b: number): number {
  const FPMIN = 1e-300;
  const EPS = 1e-14;
  const MAXIT = 300;
  const qab = a + b;
  const qap = a + 1;
  const qam = a - 1;
  let c = 1;
  let d = 1 - (qab * x) / qap;
  if (Math.abs(d) < FPMIN) d = FPMIN;
  d = 1 / d;
  let h = d;
  for (let m = 1; m <= MAXIT; m += 1) {
    const m2 = 2 * m;
    let aa = (m * (b - m) * x) / ((qam + m2) * (a + m2));
    d = 1 + aa * d;
    if (Math.abs(d) < FPMIN) d = FPMIN;
    c = 1 + aa / c;
    if (Math.abs(c) < FPMIN) c = FPMIN;
    d = 1 / d;
    h *= d * c;
    aa = (-(a + m) * (qab + m) * x) / ((a + m2) * (qap + m2));
    d = 1 + aa * d;
    if (Math.abs(d) < FPMIN) d = FPMIN;
    c = 1 + aa / c;
    if (Math.abs(c) < FPMIN) c = FPMIN;
    d = 1 / d;
    const del = d * c;
    h *= del;
    if (Math.abs(del - 1) < EPS) break;
  }
  return h;
}

export function ibeta(x: number, a: number, b: number): number {
  if (x <= 0) return 0;
  if (x >= 1) return 1;
  const lbeta = logGamma(a + b) - logGamma(a) - logGamma(b) + a * Math.log(x) + b * Math.log(1 - x);
  const front = Math.exp(lbeta);
  if (x < (a + 1) / (a + b + 2)) {
    return (front * betacf(x, a, b)) / a;
  }
  return 1 - (front * betacf(1 - x, b, a)) / b;
}

// --------------------------------------------------------------------------- //
// Student-t: CDF, survival, quantile, pdf
// --------------------------------------------------------------------------- //
export function studentTCdf(t: number, df: number): number {
  const x = df / (df + t * t);
  const half = 0.5 * ibeta(x, df / 2, 0.5);
  return t > 0 ? 1 - half : half;
}

export function studentTSf(t: number, df: number): number {
  return 1 - studentTCdf(t, df);
}

/** Two-sided p-value for an observed t: P(|T| > |t|) = I_x(df/2, 1/2). */
export function studentTwoSidedP(t: number, df: number): number {
  const x = df / (df + t * t);
  return ibeta(x, df / 2, 0.5);
}

function studentTPdf(t: number, df: number): number {
  const lg = logGamma((df + 1) / 2) - logGamma(df / 2);
  return (
    Math.exp(lg) *
    (1 / Math.sqrt(df * Math.PI)) *
    Math.pow(1 + (t * t) / df, -(df + 1) / 2)
  );
}

export function studentTInv(p: number, df: number): number {
  if (p <= 0) return -Infinity;
  if (p >= 1) return Infinity;
  // Cornish-Fisher seed off the normal quantile, then Newton on the CDF.
  const z = normalInv(p);
  const g1 = (z ** 3 + z) / 4;
  const g2 = (5 * z ** 5 + 16 * z ** 3 + 3 * z) / 96;
  let t = z + g1 / df + g2 / (df * df);
  for (let i = 0; i < 60; i += 1) {
    const err = studentTCdf(t, df) - p;
    if (Math.abs(err) < 1e-13) break;
    const step = err / studentTPdf(t, df);
    t -= step;
    if (Math.abs(step) < 1e-13) break;
  }
  return t;
}

// --------------------------------------------------------------------------- //
// noncentral-t CDF  P(T <= t | df, ncp)  — Algorithm AS 243 (Lenth 1989)
// Mirrors R's pnt.c, which is what statsmodels' power calc rests on.
// --------------------------------------------------------------------------- //
export function noncentralTCdf(t: number, df: number, ncp: number): number {
  if (!Number.isFinite(t) || !Number.isFinite(df) || !Number.isFinite(ncp)) return NaN;
  if (ncp === 0) return studentTCdf(t, df);

  let negdel = false;
  let tt = t;
  let del = ncp;
  if (t < 0) {
    negdel = true;
    tt = -t;
    del = -ncp;
  }

  let tnc = 0;
  const x = (tt * tt) / (tt * tt + df);
  if (x > 0) {
    const lambda = del * del;
    let p = 0.5 * Math.exp(-0.5 * lambda);
    let q = SQRT_2_OVER_PI * p * del;
    let s = 0.5 - p;
    if (s < 1e-7) s = -0.5 * Math.expm1(-0.5 * lambda);
    let a = 0.5;
    const b = 0.5 * df;
    const rxb = Math.pow(1 - x, b);
    const albeta = LN_SQRT_PI + logGamma(b) - logGamma(0.5 + b);
    let xodd = ibeta(x, a, b);
    let godd = 2 * rxb * Math.exp(a * Math.log(x) - albeta);
    let xeven = 1 - rxb;
    let geven = b * x * rxb;
    tnc = p * xodd + q * xeven;

    const ITRMAX = 1000;
    const ERRBD = 1e-12;
    let it = 1;
    let errbd = Infinity;
    while (it <= ITRMAX && Math.abs(errbd) > ERRBD) {
      a += 1;
      xodd -= godd;
      xeven -= geven;
      godd *= (x * (a + b - 1)) / a;
      geven *= (x * (a + b - 0.5)) / (a + 0.5);
      p *= lambda / (2 * it);
      q *= lambda / (2 * it + 1);
      s -= p;
      tnc += p * xodd + q * xeven;
      errbd = 2 * s * (xodd - godd);
      it += 1;
    }
  }
  tnc += normalCdf(-del);
  const clamped = Math.min(1, Math.max(0, tnc));
  return negdel ? 1 - clamped : clamped;
}

// --------------------------------------------------------------------------- //
// power (statsmodels TTestIndPower.power, ratio = 1, two-sided)
//   nobs = nobs1 / 2 ; df = 2*nobs1 - 2 ; nc = effect_size * sqrt(nobs)
//   power = nct.sf(crit, df, nc) + nct.cdf(-crit, df, nc) , crit = t.ppf(1-a/2, df)
// --------------------------------------------------------------------------- //
export function tTestIndPower(effectSize: number, nobs1: number, alpha: number): number {
  const df = 2 * nobs1 - 2;
  const nc = effectSize * Math.sqrt(nobs1 / 2);
  const crit = studentTInv(1 - alpha / 2, df);
  const upper = 1 - noncentralTCdf(crit, df, nc);
  const lower = noncentralTCdf(-crit, df, nc);
  return upper + lower;
}

// --------------------------------------------------------------------------- //
// analyze() — the port of ABTestEngine.analyze_experiment
// --------------------------------------------------------------------------- //
export interface AbInput {
  controlN: number;
  controlConversions: number;
  treatmentN: number;
  treatmentConversions: number;
  /** confidence_level in ABTestRequest; alpha = 1 - this. Default 0.95. */
  confidenceLevel?: number;
}

export interface AbAnalysis {
  controlRate: number;
  treatmentRate: number;
  relativeLift: number;
  tStat: number;
  df: number;
  pValue: number;
  isSignificant: boolean;
  ci95: [number, number];
  absoluteDiff: number;
  effectSize: number;
  power: number;
  powerDefined: boolean;
}

export function analyze(input: AbInput): AbAnalysis {
  const { controlN, treatmentN, controlConversions, treatmentConversions } = input;
  const confidence = input.confidenceLevel ?? 0.95;
  const alpha = 1 - confidence;

  const pC = controlConversions / controlN;
  const pT = treatmentConversions / treatmentN;
  const relativeLift = pC === 0 ? NaN : (pT - pC) / pC;

  // Welch's t-test. Sample variance of a 0/1 array (ddof = 1): n/(n-1) * p(1-p).
  const varC = (controlN / (controlN - 1)) * pC * (1 - pC);
  const varT = (treatmentN / (treatmentN - 1)) * pT * (1 - pT);
  const vnC = varC / controlN;
  const vnT = varT / treatmentN;
  const denom = Math.sqrt(vnT + vnC);
  const tStat = denom === 0 ? 0 : (pT - pC) / denom;
  const df =
    denom === 0
      ? controlN + treatmentN - 2
      : (vnT + vnC) ** 2 / (vnT ** 2 / (treatmentN - 1) + vnC ** 2 / (controlN - 1));
  const pValue = denom === 0 ? 1 : studentTwoSidedP(Math.abs(tStat), df);

  // Delta-method CI. Note the Python code uses the population SE here (ddof = 0)
  // and a *normal* quantile, not a t quantile.
  const seC = Math.sqrt((pC * (1 - pC)) / controlN);
  const seT = Math.sqrt((pT * (1 - pT)) / treatmentN);
  const seDiff = Math.sqrt(seC * seC + seT * seT);
  const z = normalInv(1 - alpha / 2);
  const diff = pT - pC;

  // Post-hoc power.
  const pooled = Math.sqrt((pC * (1 - pC) + pT * (1 - pT)) / 2);
  const effectSize = pooled === 0 ? 0 : (pT - pC) / pooled;
  const power = tTestIndPower(effectSize, controlN, alpha);
  const powerDefined = Number.isFinite(power) && power >= 0 && power <= 1;

  return {
    controlRate: pC,
    treatmentRate: pT,
    relativeLift,
    tStat,
    df,
    pValue,
    isSignificant: pValue < alpha,
    ci95: [diff - z * seDiff, diff + z * seDiff],
    absoluteDiff: diff,
    effectSize,
    power,
    powerDefined,
  };
}

/** Reconstruct integer conversion counts from a grid cell's stored rates. */
export function conversionsFromRate(visitors: number, rate: number): number {
  return Math.round(rate * visitors);
}
