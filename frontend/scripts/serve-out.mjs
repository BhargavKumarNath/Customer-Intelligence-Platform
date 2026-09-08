// Minimal static file server for the exported `out/` directory. Used by the
// Playwright config as its webServer, and handy for eyeballing a prod build.

import { createServer } from "node:http";
import { readFile, stat } from "node:fs/promises";
import { extname, join, normalize } from "node:path";

const root = join(process.cwd(), "out");
const port = Number(process.env.PORT ?? 4180);

const TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".svg": "image/svg+xml",
  ".parquet": "application/octet-stream",
  ".woff2": "font/woff2",
  ".txt": "text/plain; charset=utf-8",
  ".ico": "image/x-icon",
};

async function resolveFile(urlPath) {
  const clean = normalize(decodeURIComponent(urlPath.split("?")[0])).replace(/^(\.\.[/\\])+/, "");
  const candidates = [join(root, clean)];
  if (!extname(clean)) {
    candidates.push(join(root, clean, "index.html"), join(root, `${clean}.html`));
  }
  for (const c of candidates) {
    try {
      const s = await stat(c);
      if (s.isFile()) return c;
    } catch {
      /* try next */
    }
  }
  return null;
}

const server = createServer(async (req, res) => {
  const file = (await resolveFile(req.url ?? "/")) ?? join(root, "404.html");
  try {
    const body = await readFile(file);
    res.writeHead(file.endsWith("404.html") ? 404 : 200, {
      "Content-Type": TYPES[extname(file)] ?? "application/octet-stream",
    });
    res.end(body);
  } catch {
    res.writeHead(500);
    res.end("server error");
  }
});

server.listen(port, () => console.log(`[serve-out] http://localhost:${port} -> ${root}`));
