"use client";

import { useEffect } from "react";
import { ErrorState } from "@/components/ui/states";

export default function RouteError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error(error);
  }, [error]);

  return (
    <div className="max-w-prose">
      <span className="kicker">Something broke</span>
      <h1 className="mt-4 text-3xl">This section could not render.</h1>
      <p className="mt-3 text-ink-muted">
        The data build may be incomplete, or a chart hit an unexpected value. The rest of the site
        still works.
      </p>
      <div className="mt-6">
        <ErrorState title="Render error" onRetry={reset}>
          {error.message}
        </ErrorState>
      </div>
    </div>
  );
}
