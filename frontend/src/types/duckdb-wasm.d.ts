// The package's `exports` map does not wire types to the browser dist subpath,
// so map it back to the package root types (which do resolve). We import that
// subpath directly to dodge the Node build's dynamic require (webpack "critical
// dependency" warning).
declare module "@duckdb/duckdb-wasm/dist/duckdb-browser" {
  export * from "@duckdb/duckdb-wasm";
}
