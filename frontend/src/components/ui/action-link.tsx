import Link from "next/link";
import { ArrowRightIcon, ArrowUpRightIcon } from "@/components/icons";
import { cn } from "@/lib/cn";

export function ActionLink({
  href,
  children,
  external = false,
  className,
}: {
  href: string;
  children: React.ReactNode;
  external?: boolean;
  className?: string;
}) {
  const Icon = external ? ArrowUpRightIcon : ArrowRightIcon;
  const cls = cn(
    "group inline-flex items-center gap-1.5 text-sm font-medium text-accent-ink transition-colors hover:text-accent",
    className,
  );
  const inner = (
    <>
      {children}
      <Icon size={15} className="transition-transform duration-200 group-hover:translate-x-0.5" />
    </>
  );
  if (external) {
    return (
      <a href={href} target="_blank" rel="noreferrer" className={cls}>
        {inner}
      </a>
    );
  }
  return (
    <Link href={href} className={cls}>
      {inner}
    </Link>
  );
}
