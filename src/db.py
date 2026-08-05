from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache

import duckdb

from src.config import Settings, get_settings


class DuckDBConnectionManager:
    """Owns a single read-only DuckDB connection and hands out per-call cursors.

    DuckDB connections aren't safe to share across concurrent queries, but
    `.cursor()` creates a lightweight cursor over the same database that is
    — that's what lets multiple threadpool-offloaded requests run concurrently
    without each opening its own file handle.
    """

    def __init__(self, settings: Settings) -> None:
        if not settings.database_path.exists():
            raise FileNotFoundError(f"DuckDB database not found at {settings.database_path}")
        self._connection = duckdb.connect(str(settings.database_path), read_only=True)
        self._connection.execute(f"SET memory_limit='{settings.memory_limit}';")
        self._connection.execute(f"SET threads TO {settings.threads};")

    @contextmanager
    def cursor(self) -> Iterator[duckdb.DuckDBPyConnection]:
        cur = self._connection.cursor()
        try:
            yield cur
        finally:
            cur.close()

    def close(self) -> None:
        self._connection.close()


@lru_cache(maxsize=1)
def get_connection_manager() -> DuckDBConnectionManager:
    return DuckDBConnectionManager(get_settings())
