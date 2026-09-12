"use client";

/**
 * Throwaway page for the Phase 8 Sentry verification test case
 * (deployment_stages.md): confirms a real exception reaches Sentry with the
 * correct release tag before relying on it for alerting. Not linked from any
 * nav. Delete this route once verified against a deploy with
 * NEXT_PUBLIC_SENTRY_DSN set — it has no purpose afterwards.
 */
export default function DebugSentryPage() {
  return (
    <div className="max-w-prose">
      <span className="kicker">Debug</span>
      <h1 className="mt-4 text-3xl">Sentry test page</h1>
      <p className="mt-3 text-ink-muted">
        Throws on click, to confirm error tracking is wired to a real Sentry project. Delete this
        route after verifying the event arrives.
      </p>
      <button
        type="button"
        className="mt-6 rounded-sm border border-rule px-3 py-1.5 text-sm text-ink transition-colors hover:border-rule-strong"
        onClick={() => {
          throw new Error("debug-sentry: manual test exception");
        }}
      >
        Throw test exception
      </button>
    </div>
  );
}
