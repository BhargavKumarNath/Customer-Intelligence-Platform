/** The route spine. Ordered as the analytical narrative, not as the Streamlit sidebar. */

export interface NavItem {
  href: string;
  index: string;
  title: string;
  kicker: string;
  summary: string;
}

export const NAV: NavItem[] = [
  {
    href: "/",
    index: "00",
    title: "The read",
    kicker: "Start here",
    summary: "What the data says and what to do about it.",
  },
  {
    href: "/executive",
    index: "01",
    title: "Business performance",
    kicker: "What happened",
    summary: "Revenue, orders, and funnel health across the tracked window.",
  },
  {
    href: "/user-intelligence",
    index: "02",
    title: "User intelligence",
    kicker: "Who is buying",
    summary: "RFM segments, retention cohorts, and a single-user lookup.",
  },
  {
    href: "/ml-engine",
    index: "03",
    title: "Propensity and affinity",
    kicker: "Where to focus",
    summary: "The purchase model, its drivers, and product co-purchase rules.",
  },
  {
    href: "/experiments",
    index: "04",
    title: "Experiment lab",
    kicker: "How to decide",
    summary: "A calculator for the reactivation test, checked against a precomputed grid.",
  },
  {
    href: "/data-explorer",
    index: "05",
    title: "Data explorer",
    kicker: "Look closer",
    summary: "Run SQL against the event log in the browser.",
  },
  {
    href: "/overview",
    index: "06",
    title: "Method and glossary",
    kicker: "How it is built",
    summary: "Pipeline, star schema, and the terms used throughout.",
  },
];

export const PRIMARY_NAV = NAV;

export function navByHref(href: string): NavItem | undefined {
  return NAV.find((n) => n.href === href);
}
