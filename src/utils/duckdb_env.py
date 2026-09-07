"""Shared DuckDB session tuning for the offline full-scale pipeline.

The Hydra pipeline scripts (``src/ingestion``, ``src/processing``,
``src/analysis``, ``src/models``) each open a DuckDB connection against the
full ~110M-row ``data/db/behavior.duckdb`` and used to hard-code a
``SET memory_limit='10GB'`` / ``'12GB'`` that only made sense on the original
16 GB dev machine. On anything smaller those limits let DuckDB commit more
memory than the box has and the OS killer steps in.

This centralises the policy: bounded memory, disk spill enabled, insertion
order not preserved (none of these aggregations depend on it). Every value is
overridable by env var so the same code runs unchanged on a laptop or a
bigger machine::

    CIP_DUCKDB_MEMORY_LIMIT   default "3GB"
    CIP_DUCKDB_THREADS        default "2"
    CIP_DUCKDB_TEMP_DIR       default "<db_dir>/.duckdb_spill" (or CWD)

The defaults are deliberately small. The full pipeline runs the ~110M-row
event log through several GROUP BYs whose peak footprint DuckDB can only keep
near ``memory_limit`` when it is free to spill and not fanned out across many
threads. Earlier defaults (6-12 GB, 4 threads) drove process RSS past the
limit and swap-thrashed a 10 GB box to a hard freeze. 3 GB / 2 threads
completes every stage with headroom; bump the env vars on a larger machine
for speed.
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

DEFAULT_MEMORY_LIMIT = "3GB"
DEFAULT_THREADS = "2"


def apply_pragmas(
    con: duckdb.DuckDBPyConnection,
    *,
    db_path: str | os.PathLike[str] | None = None,
    preserve_insertion_order: bool = False,
) -> None:
    """Apply the shared memory/threads/temp-dir PRAGMAs to ``con``."""
    memory_limit = os.environ.get("CIP_DUCKDB_MEMORY_LIMIT", DEFAULT_MEMORY_LIMIT)
    threads = os.environ.get("CIP_DUCKDB_THREADS", DEFAULT_THREADS)

    con.execute(f"SET memory_limit='{memory_limit}';")
    con.execute(f"SET threads TO {threads};")
    if not preserve_insertion_order:
        con.execute("SET preserve_insertion_order=false;")

    temp_dir = os.environ.get("CIP_DUCKDB_TEMP_DIR")
    if temp_dir is None and db_path is not None:
        temp_dir = str(Path(db_path).resolve().parent / ".duckdb_spill")
    if temp_dir:
        Path(temp_dir).mkdir(parents=True, exist_ok=True)
        con.execute(f"SET temp_directory='{temp_dir}';")
