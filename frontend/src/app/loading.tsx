import { Skeleton } from "@/components/ui/states";

export default function Loading() {
  return (
    <div className="max-w-prose animate-pulse">
      <Skeleton className="h-3 w-24" />
      <Skeleton className="mt-5 h-12 w-full" />
      <Skeleton className="mt-3 h-12 w-3/4" />
      <Skeleton className="mt-8 h-5 w-full" />
      <Skeleton className="mt-2 h-5 w-5/6" />
      <div className="mt-12 grid gap-px rounded-md border border-rule bg-rule sm:grid-cols-4">
        {Array.from({ length: 4 }).map((_, i) => (
          <div key={i} className="bg-surface p-5">
            <Skeleton className="h-3 w-16" />
            <Skeleton className="mt-3 h-8 w-24" />
          </div>
        ))}
      </div>
    </div>
  );
}
