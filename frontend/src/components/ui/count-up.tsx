"use client";

import { useEffect, useRef, useState } from "react";
import { useInView, useReducedMotion } from "framer-motion";

/**
 * Counts a headline figure up when it scrolls into view. It renders the final
 * value on the server and on first paint, so the real number is always in the
 * DOM (SEO, no-JS, and the number-parity tests); the animation only replays it.
 */
export function CountUp({
  value,
  format,
  durationMs = 900,
  className,
}: {
  value: number;
  format: (n: number) => string;
  durationMs?: number;
  className?: string;
}) {
  const reduce = useReducedMotion();
  const ref = useRef<HTMLSpanElement>(null);
  const inView = useInView(ref, { once: true, margin: "-40px" });
  const [display, setDisplay] = useState(value);
  const played = useRef(false);

  useEffect(() => {
    if (reduce || !inView || played.current) {
      setDisplay(value);
      return;
    }
    played.current = true;
    let raf = 0;
    const start = performance.now();
    const tick = (now: number) => {
      const t = Math.min(1, (now - start) / durationMs);
      const eased = 1 - Math.pow(1 - t, 4);
      setDisplay(value * eased);
      if (t < 1) raf = requestAnimationFrame(tick);
      else setDisplay(value);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [inView, value, durationMs, reduce]);

  return (
    <span ref={ref} className={className}>
      {format(display)}
    </span>
  );
}
