import type { ReactNode } from "react";
import { cn } from "@/lib/cn";
import { AlertIcon, SpinnerIcon } from "@/components/icons";

export function Skeleton({ className, style }: { className?: string; style?: React.CSSProperties }) {
  return <div className={cn("skeleton", className)} style={style} aria-hidden />;
}

export function ChartSkeleton({ height = 280 }: { height?: number }) {
  return (
    <div className="space-y-3" aria-hidden>
      <Skeleton className="h-3 w-32" />
      <Skeleton style={{ height }} className="w-full" />
      <div className="flex gap-2">
        <Skeleton className="h-3 w-16" />
        <Skeleton className="h-3 w-16" />
        <Skeleton className="h-3 w-16" />
      </div>
    </div>
  );
}

export function LoadingBlock({ label = "Loading" }: { label?: string }) {
  return (
    <div className="flex items-center gap-2.5 py-10 text-sm text-ink-muted" role="status">
      <SpinnerIcon size={16} />
      <span>{label}</span>
    </div>
  );
}

export function EmptyState({
  title,
  children,
  icon,
}: {
  title: string;
  children?: ReactNode;
  icon?: ReactNode;
}) {
  return (
    <div className="flex flex-col items-center justify-center rounded-md border border-dashed border-rule-strong px-6 py-12 text-center">
      {icon && <div className="mb-3 text-ink-faint">{icon}</div>}
      <p className="font-display text-base text-ink">{title}</p>
      {children && <p className="mt-1.5 max-w-sm text-sm text-ink-muted">{children}</p>}
    </div>
  );
}

export function ErrorState({
  title = "Something did not load",
  children,
  onRetry,
}: {
  title?: string;
  children?: ReactNode;
  onRetry?: () => void;
}) {
  return (
    <div className="panel border-l-2 border-l-critical p-5">
      <div className="flex items-start gap-3">
        <span className="mt-0.5 text-critical">
          <AlertIcon size={18} />
        </span>
        <div>
          <p className="font-medium text-ink">{title}</p>
          {children && <p className="mt-1 text-sm text-ink-muted">{children}</p>}
          {onRetry && (
            <button
              type="button"
              onClick={onRetry}
              className="mt-3 rounded-sm border border-rule px-2.5 py-1 text-xs text-ink-muted transition-colors hover:border-rule-strong hover:text-ink"
            >
              Try again
            </button>
          )}
        </div>
      </div>
    </div>
  );
}
