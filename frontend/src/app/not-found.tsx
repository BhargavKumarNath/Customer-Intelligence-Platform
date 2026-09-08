import Link from "next/link";
import { NAV } from "@/lib/nav";
import { ArrowRightIcon } from "@/components/icons";

export default function NotFound() {
  return (
    <div className="max-w-prose">
      <span className="kicker">Error 404</span>
      <h1 className="mt-4 text-4xl">That page is not part of this report.</h1>
      <p className="mt-4 text-lg text-ink-muted">
        The link may be out of date. Pick up the thread from one of the sections below.
      </p>
      <ul className="mt-8 divide-y divide-rule border-y border-rule">
        {NAV.map((item) => (
          <li key={item.href}>
            <Link
              href={item.href}
              className="group flex items-center justify-between gap-4 py-3.5 transition-colors hover:text-accent-ink"
            >
              <span className="flex items-baseline gap-3">
                <span className="tnum text-2xs text-ink-faint">{item.index}</span>
                <span className="text-sm font-medium">{item.title}</span>
              </span>
              <ArrowRightIcon
                size={16}
                className="text-ink-faint transition-transform group-hover:translate-x-0.5 group-hover:text-accent-ink"
              />
            </Link>
          </li>
        ))}
      </ul>
    </div>
  );
}
