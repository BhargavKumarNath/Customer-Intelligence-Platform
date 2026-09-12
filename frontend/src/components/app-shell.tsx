"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { AnimatePresence, motion, useReducedMotion } from "framer-motion";
import { NAV } from "@/lib/nav";
import { shortSha } from "@/lib/format";
import { cn } from "@/lib/cn";
import { ThemeToggle } from "./theme-toggle";
import { CloseIcon, MenuIcon } from "./icons";

interface BuildInfo {
  gitSha: string;
  builtAt: string;
  datasetRows: number;
  dateRange: [string, string];
  syncedAt: string;
}

export function AppShell({ children, build }: { children: React.ReactNode; build: BuildInfo }) {
  const pathname = usePathname();
  const [drawerOpen, setDrawerOpen] = useState(false);

  useEffect(() => {
    setDrawerOpen(false);
  }, [pathname]);

  useEffect(() => {
    document.body.style.overflow = drawerOpen ? "hidden" : "";
    return () => {
      document.body.style.overflow = "";
    };
  }, [drawerOpen]);

  return (
    <div className="lg:grid lg:grid-cols-[17rem_minmax(0,1fr)]">
      <aside className="sticky top-0 hidden h-screen flex-col border-r border-rule bg-surface lg:flex">
        <RailContent build={build} pathname={pathname} />
      </aside>

      <TopBar onMenu={() => setDrawerOpen(true)} />

      <AnimatePresence>
        {drawerOpen && <MobileDrawer build={build} pathname={pathname} onClose={() => setDrawerOpen(false)} />}
      </AnimatePresence>

      <div className="min-w-0">
        <main id="main" className="shell py-10 lg:py-16">
          {children}
        </main>
        <SiteFooter build={build} />
      </div>
    </div>
  );
}

function Wordmark() {
  return (
    <Link href="/" className="group flex items-baseline gap-2">
      <span className="font-display text-lg leading-none text-ink">Customer Intelligence</span>
      <span
        aria-hidden
        className="h-2 w-2 translate-y-[-1px] rounded-full bg-accent transition-transform duration-300 ease-out-quint group-hover:scale-125"
      />
    </Link>
  );
}

function RailContent({ build, pathname }: { build: BuildInfo; pathname: string }) {
  return (
    <>
      <div className="border-b border-rule px-6 py-6">
        <Wordmark />
        <p className="mt-2 max-w-[22ch] text-2xs leading-relaxed text-ink-faint">
          Static read of a {new Intl.NumberFormat("en-US").format(build.datasetRows)}-event log
        </p>
      </div>

      <nav className="flex-1 overflow-y-auto px-3 py-4" aria-label="Sections">
        <ul className="space-y-0.5">
          {NAV.map((item) => {
            const active = pathname === item.href;
            return (
              <li key={item.href}>
                <Link
                  href={item.href}
                  aria-current={active ? "page" : undefined}
                  className={cn(
                    "group relative block rounded-md px-3 py-2.5 transition-colors duration-150",
                    active ? "bg-accent-soft" : "hover:bg-surface-sunken",
                  )}
                >
                  <span
                    aria-hidden
                    className={cn(
                      "absolute left-0 top-1/2 h-6 w-[3px] -translate-y-1/2 rounded-full bg-accent transition-opacity duration-200",
                      active ? "opacity-100" : "opacity-0",
                    )}
                  />
                  <span className="flex items-baseline gap-2.5">
                    <span className="tnum text-2xs text-ink-faint">{item.index}</span>
                    <span
                      className={cn(
                        "text-sm font-medium",
                        active ? "text-accent-ink" : "text-ink group-hover:text-ink",
                      )}
                    >
                      {item.title}
                    </span>
                  </span>
                  <span className="mt-0.5 block pl-[2.1rem] text-2xs leading-snug text-ink-faint">
                    {item.kicker}
                  </span>
                </Link>
              </li>
            );
          })}
        </ul>
      </nav>

      <div className="border-t border-rule px-6 py-5">
        <BuildStamp build={build} />
        <div className="mt-4">
          <ThemeToggle />
        </div>
      </div>
    </>
  );
}

function BuildStamp({ build }: { build: BuildInfo }) {
  const built = new Date(build.builtAt);
  return (
    <dl className="space-y-1.5 text-2xs text-ink-faint">
      <div className="flex justify-between gap-3">
        <dt>Data build</dt>
        <dd className="tnum text-ink-muted">{shortSha(build.gitSha)}</dd>
      </div>
      <div className="flex justify-between gap-3">
        <dt>Compiled</dt>
        <dd className="tnum text-ink-muted">
          {built.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "2-digit", timeZone: "UTC" })}
        </dd>
      </div>
      <div className="flex justify-between gap-3">
        <dt>Window</dt>
        <dd className="tnum text-ink-muted">
          {build.dateRange[0]} to {build.dateRange[1]}
        </dd>
      </div>
    </dl>
  );
}

function TopBar({ onMenu }: { onMenu: () => void }) {
  return (
    <header className="sticky top-0 z-30 flex items-center justify-between border-b border-rule bg-paper/85 px-4 py-3 backdrop-blur lg:hidden">
      <button
        type="button"
        onClick={onMenu}
        className="flex h-9 w-9 items-center justify-center rounded-md border border-rule text-ink-muted"
        aria-label="Open navigation"
      >
        <MenuIcon size={18} />
      </button>
      <Wordmark />
      <ThemeToggle compact />
    </header>
  );
}

function MobileDrawer({
  build,
  pathname,
  onClose,
}: {
  build: BuildInfo;
  pathname: string;
  onClose: () => void;
}) {
  const reduce = useReducedMotion();
  return (
    <motion.div
      className="fixed inset-0 z-40 lg:hidden"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: reduce ? 0 : 0.16 }}
    >
      <div className="absolute inset-0 bg-ink/40" onClick={onClose} aria-hidden />
      <motion.aside
        className="absolute left-0 top-0 flex h-full w-[19rem] max-w-[86vw] flex-col bg-surface shadow-xl"
        initial={{ x: reduce ? 0 : "-100%" }}
        animate={{ x: 0 }}
        exit={{ x: reduce ? 0 : "-100%" }}
        transition={{ type: "tween", ease: [0.22, 1, 0.36, 1], duration: reduce ? 0 : 0.28 }}
      >
        <div className="flex justify-end p-3">
          <button
            type="button"
            onClick={onClose}
            className="flex h-9 w-9 items-center justify-center rounded-md border border-rule text-ink-muted"
            aria-label="Close navigation"
          >
            <CloseIcon size={18} />
          </button>
        </div>
        <RailContent build={build} pathname={pathname} />
      </motion.aside>
    </motion.div>
  );
}

function SiteFooter({ build }: { build: BuildInfo }) {
  return (
    <footer className="mt-16 border-t border-rule">
      <div className="shell flex flex-col gap-3 py-8 text-2xs text-ink-faint sm:flex-row sm:items-center sm:justify-between">
        <p className="max-w-prose">
          Every figure on this site is derived from the pinned data build. Nothing is typed in by
          hand. Source dataset is a public e-commerce event log from October and November 2019.
        </p>
        <p className="tnum shrink-0">
          build {shortSha(build.gitSha)} · synced{" "}
          {new Date(build.syncedAt).toLocaleDateString("en-US", { month: "short", day: "2-digit", year: "numeric" })}
        </p>
      </div>
    </footer>
  );
}
