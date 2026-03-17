"""
acropora_store/archival.py

Nightly Parquet archival pipeline.

Moves data from the TimescaleDB hot tier to Parquet files on disk
before the retention policy drops it. Runs as a scheduled job inside
the acropora-archival Docker container.

Usage:
    python -m acropora_store.archival --schedule      # nightly at 2am
    python -m acropora_store.archival --run-once      # run immediately and exit

Design principles:
    - Raw full-precision data is archived — no privacy transforms applied
      Privacy is applied at query time by duckdb_query.py
    - Atomic writes — temp file written first, renamed after verification
    - Idempotent — safe to run multiple times, re-runs re-export cleanly
    - Sequential chunk processing — simpler and sufficient for homelab scale

Output directory structure:
    {archive_path}/{payload_type}/{YYYY}/{MM}/{YYYY-MM-DD}.parquet
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path

import asyncpg
import pyarrow as pa
import pyarrow.parquet as pq
from acropora_store.schema import PARQUET_SCHEMAS, StorageConfig, coral_core_schema

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Data structures
# ─────────────────────────────────────────────


@dataclass
class ChunkInfo:
    """
    Metadata about one (TimescaleDB chunk, payload_type) pair
    ready for archival.

    A single time-based chunk may contain multiple payload_types.
    We produce one Parquet file per payload_type per day, so each
    chunk generates one ChunkInfo per distinct payload_type it holds.
    """

    chunk_id: int
    chunk_schema: str
    chunk_name: str
    range_start: int  # timestamp_us — inclusive
    range_end: int  # timestamp_us — exclusive
    payload_type: str
    estimated_rows: int

    @property
    def date_str(self) -> str:
        """YYYY-MM-DD of the chunk's start timestamp."""
        dt = datetime.fromtimestamp(
            self.range_start / 1_000_000,
            tz=UTC,
        )
        return dt.strftime("%Y-%m-%d")

    @property
    def year_str(self) -> str:
        dt = datetime.fromtimestamp(
            self.range_start / 1_000_000,
            tz=UTC,
        )
        return dt.strftime("%Y")

    @property
    def month_str(self) -> str:
        dt = datetime.fromtimestamp(
            self.range_start / 1_000_000,
            tz=UTC,
        )
        return dt.strftime("%m")


@dataclass
class ArchivalResult:
    """Result of archiving one (chunk, payload_type) pair."""

    chunk_id: int
    payload_type: str
    date_str: str
    parquet_path: str
    record_count: int
    file_size_bytes: int
    checksum_sha256: str
    duration_ms: float
    success: bool
    error: str | None = None


@dataclass
class ArchivalSummary:
    """Aggregate result of one full archival pass."""

    chunks_found: int
    chunks_succeeded: int
    chunks_skipped: int  # Empty chunks
    chunks_failed: int
    total_records: int
    total_bytes: int
    duration_ms: float
    errors: list[str] = field(default_factory=list)


# ─────────────────────────────────────────────
# Chunk discovery
# ─────────────────────────────────────────────


async def find_archivable_chunks(
    conn: asyncpg.Connection,
    archive_after_days: int,
) -> list[ChunkInfo]:
    """
    Find TimescaleDB chunks ready for archival.

    A chunk is archivable when:
    - Its end timestamp is older than archive_after_days
    - It has no entry in archival_log for this payload_type
    - It contains at least one row (estimated_row_count > 0)

    Returns one ChunkInfo per (chunk_id, payload_type) pair.
    A chunk with both adsb and ais data returns two ChunkInfos.
    """
    cutoff_us = int((time.time() - archive_after_days * 86400) * 1_000_000)

    # Find chunks older than the cutoff not yet archived
    chunk_rows = await conn.fetch(
        """
        SELECT
            c.id                    AS chunk_id,
            c.chunk_schema,
            c.chunk_name,
            c.hypertable_name       AS table_name,
            c.range_start,
            c.range_end,
            c.estimated_row_count   AS estimated_rows
        FROM timescaledb_information.chunks c
        WHERE
            c.hypertable_name = 'observations'
            AND c.range_end < $1
            AND c.estimated_row_count > 0
        ORDER BY c.range_start ASC
    """,
        cutoff_us,
    )

    if not chunk_rows:
        return []

    chunks: list[ChunkInfo] = []

    for row in chunk_rows:
        chunk_id = row["chunk_id"]

        # Find which payload_types exist in this chunk
        # and which have not yet been archived
        payload_rows = await conn.fetch(
            """
            SELECT DISTINCT o.payload_type
            FROM observations o
            WHERE
                o.timestamp_us >= $1
                AND o.timestamp_us < $2
                AND NOT EXISTS (
                    SELECT 1 FROM archival_log al
                    WHERE al.chunk_id = $3
                    AND al.payload_type = o.payload_type
                )
        """,
            row["range_start"],
            row["range_end"],
            chunk_id,
        )

        for pt_row in payload_rows:
            chunks.append(
                ChunkInfo(
                    chunk_id=chunk_id,
                    chunk_schema=row["chunk_schema"],
                    chunk_name=row["chunk_name"],
                    range_start=row["range_start"],
                    range_end=row["range_end"],
                    payload_type=pt_row["payload_type"],
                    estimated_rows=row["estimated_rows"],
                )
            )

    return chunks


# ─────────────────────────────────────────────
# Data export
# ─────────────────────────────────────────────


async def fetch_chunk_rows(
    conn: asyncpg.Connection,
    chunk: ChunkInfo,
) -> list[asyncpg.Record]:
    """
    Fetch all raw stored records for a (chunk, payload_type) pair.

    Fetches full-precision raw data — no privacy transforms.
    Privacy is applied at query time by duckdb_query.py.

    For ADS-B, payload fields are fetched as separate columns
    for efficient columnar Parquet storage.
    For other types, payload is fetched as a JSON string.
    """
    if chunk.payload_type == "adsb":
        return await conn.fetch(
            """
            SELECT
                timestamp_us,
                lat,
                lon,
                altitude_m,
                altitude_type,
                source_id,
                record_id,
                node_id,
                payload_type,
                payload_schema,
                payload_schema_version,
                ingested_at_us,
                signed_at_us,
                signature,
                tags::text                              AS tags,
                (payload->>'callsign')::text            AS callsign,
                (payload->>'squawk')::text              AS squawk,
                (payload->>'altitude_baro_ft')::int     AS altitude_baro_ft,
                (payload->>'ground_speed_kt')::float    AS ground_speed_kt,
                (payload->>'track_deg')::float          AS track_deg,
                (payload->>'vertical_rate_fpm')::int    AS vertical_rate_fpm,
                (payload->>'nic')::smallint             AS nic,
                (payload->>'nac_p')::smallint           AS nac_p,
                (payload->>'category')::text            AS category,
                (payload->>'rssi_dbm')::float           AS rssi_dbm,
                (payload->>'messages_seen')::int        AS messages_seen
            FROM observations
            WHERE
                timestamp_us >= $1
                AND timestamp_us < $2
                AND payload_type = 'adsb'
            ORDER BY source_id ASC, timestamp_us ASC
        """,
            chunk.range_start,
            chunk.range_end,
        )

    else:
        return await conn.fetch(
            """
            SELECT
                timestamp_us,
                lat,
                lon,
                altitude_m,
                altitude_type,
                source_id,
                record_id,
                node_id,
                payload_type,
                payload_schema,
                payload_schema_version,
                ingested_at_us,
                signed_at_us,
                signature,
                tags::text          AS tags,
                payload::text       AS payload_json
            FROM observations
            WHERE
                timestamp_us >= $1
                AND timestamp_us < $2
                AND payload_type = $3
            ORDER BY source_id ASC, timestamp_us ASC
        """,
            chunk.range_start,
            chunk.range_end,
            chunk.payload_type,
        )


def rows_to_arrow_table(
    rows: list[asyncpg.Record],
    payload_type: str,
) -> pa.Table:
    """
    Convert asyncpg rows to a PyArrow table using the appropriate schema.

    Builds column-oriented arrays from row-oriented asyncpg records.
    Uses the PARQUET_SCHEMAS registry to get the correct schema per type.
    """
    schema = PARQUET_SCHEMAS.get(payload_type, coral_core_schema)

    if not rows:
        # Return empty table with correct schema
        return pa.table(
            {field.name: pa.array([], type=field.type) for field in schema},
            schema=schema,
        )

    # Build column-oriented dict from row-oriented records
    columns: dict[str, list] = {field.name: [] for field in schema}

    for row in rows:
        row_dict = dict(row)
        for schema_field in schema:
            value = row_dict.get(schema_field.name)
            columns[schema_field.name].append(value)

    # Build typed PyArrow arrays — fall back to null array on type errors
    arrays: list[pa.Array] = []
    for schema_field in schema:
        try:
            arr = pa.array(columns[schema_field.name], type=schema_field.type)
        except (pa.ArrowInvalid, pa.ArrowTypeError) as e:
            logger.warning(
                f"Type error for column '{schema_field.name}' "
                f"(payload_type={payload_type}): {e}. "
                f"Falling back to null array."
            )
            arr = pa.nulls(len(rows), type=schema_field.type)
        arrays.append(arr)

    return pa.table(arrays, schema=schema)


def compute_file_checksum(path: Path) -> str:
    """SHA-256 checksum of a file. Returns hex digest string."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk_bytes in iter(lambda: f.read(65_536), b""):
            h.update(chunk_bytes)
    return h.hexdigest()


async def export_chunk(
    conn: asyncpg.Connection,
    chunk: ChunkInfo,
    archive_path: Path,
    compression: str = "zstd",
) -> ArchivalResult:
    """
    Export one (chunk, payload_type) pair to a Parquet file.

    Write sequence:
    1. Fetch rows from TimescaleDB
    2. Convert to PyArrow table
    3. Write to {path}.tmp
    4. Verify row count by reading back
    5. Compute SHA-256 checksum
    6. Atomic rename .tmp → .parquet
    7. fsync the directory

    If any step fails, the .tmp file is cleaned up and a failed
    ArchivalResult is returned. The chunk will be retried next run.
    """
    start_ms = time.time() * 1000

    # Build output path
    output_dir = archive_path / chunk.payload_type / chunk.year_str / chunk.month_str
    output_dir.mkdir(parents=True, exist_ok=True)

    final_path = output_dir / f"{chunk.date_str}.parquet"
    temp_path = output_dir / f"{chunk.date_str}.parquet.tmp"

    logger.info(
        f"Archiving chunk {chunk.chunk_id} "
        f"({chunk.payload_type}, {chunk.date_str}) "
        f"→ {final_path}"
    )

    try:
        # ── Step 1: Fetch rows ───────────────────────────────
        rows = await fetch_chunk_rows(conn, chunk)

        if not rows:
            # Empty chunk — record in archival_log to skip next time
            logger.info(
                f"Chunk {chunk.chunk_id} ({chunk.payload_type}) "
                f"is empty — skipping Parquet write"
            )
            duration_ms = time.time() * 1000 - start_ms
            return ArchivalResult(
                chunk_id=chunk.chunk_id,
                payload_type=chunk.payload_type,
                date_str=chunk.date_str,
                parquet_path=str(final_path),
                record_count=0,
                file_size_bytes=0,
                checksum_sha256="",
                duration_ms=duration_ms,
                success=True,
            )

        # ── Step 2: Convert to PyArrow table ────────────────
        table = rows_to_arrow_table(rows, chunk.payload_type)

        # ── Step 3: Write to temp file ───────────────────────
        pq.write_table(
            table,
            temp_path,
            compression=compression,
            # Write column statistics — enables predicate pushdown
            # when DuckDB reads these files
            write_statistics=True,
            # Row group size — 1M rows max, good balance for DuckDB
            row_group_size=min(len(rows), 1_000_000),
        )

        # ── Step 4: Verify row count ─────────────────────────
        verified = pq.read_table(temp_path)
        if len(verified) != len(rows):
            raise ValueError(
                f"Verification failed: wrote {len(rows)} rows "
                f"but read back {len(verified)}"
            )

        # ── Step 5: Compute checksum ─────────────────────────
        checksum = compute_file_checksum(temp_path)
        file_size = temp_path.stat().st_size

        # ── Step 6: Atomic rename ────────────────────────────
        # If a Parquet file already exists (previous partial run)
        # this replaces it with the newly verified file
        temp_path.rename(final_path)

        # ── Step 7: fsync directory ──────────────────────────
        # Ensures the rename is durable on the filesystem
        if sys.platform != "win32":
            dir_fd = os.open(str(output_dir), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)

        duration_ms = time.time() * 1000 - start_ms

        logger.info(
            f"Archived {len(rows):,} rows → {final_path.name} "
            f"({file_size / 1_048_576:.1f} MB, {duration_ms:.0f}ms)"
        )

        return ArchivalResult(
            chunk_id=chunk.chunk_id,
            payload_type=chunk.payload_type,
            date_str=chunk.date_str,
            parquet_path=str(final_path),
            record_count=len(rows),
            file_size_bytes=file_size,
            checksum_sha256=checksum,
            duration_ms=duration_ms,
            success=True,
        )

    except Exception as e:
        # Clean up temp file if it exists
        if temp_path.exists():
            try:
                temp_path.unlink()
            except OSError:
                pass

        duration_ms = time.time() * 1000 - start_ms
        logger.error(
            f"Failed to archive chunk {chunk.chunk_id} ({chunk.payload_type}): {e}",
            exc_info=True,
        )

        return ArchivalResult(
            chunk_id=chunk.chunk_id,
            payload_type=chunk.payload_type,
            date_str=chunk.date_str,
            parquet_path=str(final_path),
            record_count=0,
            file_size_bytes=0,
            checksum_sha256="",
            duration_ms=duration_ms,
            success=False,
            error=str(e),
        )


# ─────────────────────────────────────────────
# Archival log
# ─────────────────────────────────────────────


async def record_archival(
    conn: asyncpg.Connection,
    result: ArchivalResult,
    chunk: ChunkInfo,
) -> None:
    """
    Record a completed archival in archival_log.
    Uses ON CONFLICT DO UPDATE so re-runs are idempotent.
    Called for both successful exports and empty chunks.
    """
    await conn.execute(
        """
        INSERT INTO archival_log (
            chunk_id,
            chunk_start_us,
            chunk_end_us,
            payload_type,
            record_count,
            parquet_path,
            parquet_size_bytes,
            archived_at_us,
            checksum_sha256
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (chunk_id) DO UPDATE SET
            parquet_path        = EXCLUDED.parquet_path,
            parquet_size_bytes  = EXCLUDED.parquet_size_bytes,
            archived_at_us      = EXCLUDED.archived_at_us,
            checksum_sha256     = EXCLUDED.checksum_sha256,
            record_count        = EXCLUDED.record_count
    """,
        chunk.chunk_id,
        chunk.range_start,
        chunk.range_end,
        result.payload_type,
        result.record_count,
        result.parquet_path,
        result.file_size_bytes,
        time.time_ns() // 1_000,
        result.checksum_sha256,
    )


# ─────────────────────────────────────────────
# Archival job
# ─────────────────────────────────────────────


class ArchivalJob:
    """
    Nightly Parquet archival job.

    Finds archivable chunks, exports them sequentially,
    records results in archival_log, and optionally runs
    on a nightly schedule.

    Usage:
        job = ArchivalJob(pool=pool, config=config)
        await job.run_once()                        # manual / testing
        await job.run_scheduled(hour=2, minute=0)   # production
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        config: StorageConfig,
    ) -> None:
        self.pool = pool
        self.config = config
        self.archive_path = Path(config.archive_path)

    async def run_once(self) -> ArchivalSummary:
        """
        Run one archival pass — process all pending chunks.

        Returns an ArchivalSummary with counts and totals.
        Never raises — errors are captured in the summary.
        """
        if not self.config.archive_enabled:
            logger.info("Archival disabled in config — skipping")
            return ArchivalSummary(
                chunks_found=0,
                chunks_succeeded=0,
                chunks_skipped=0,
                chunks_failed=0,
                total_records=0,
                total_bytes=0,
                duration_ms=0,
            )

        pass_start_ms = time.time() * 1000
        logger.info(
            f"Starting archival pass "
            f"(archive_after_days={self.config.archive_after_days})"
        )

        # ── Discover archivable chunks ───────────────────────
        async with self.pool.acquire() as conn:
            chunks = await find_archivable_chunks(
                conn=conn,
                archive_after_days=self.config.archive_after_days,
            )

        if not chunks:
            logger.info("No chunks ready for archival")
            return ArchivalSummary(
                chunks_found=0,
                chunks_succeeded=0,
                chunks_skipped=0,
                chunks_failed=0,
                total_records=0,
                total_bytes=0,
                duration_ms=time.time() * 1000 - pass_start_ms,
            )

        logger.info(f"Found {len(chunks)} chunk(s) to archive")

        # ── Process each chunk sequentially ─────────────────
        results: list[ArchivalResult] = []

        for chunk in chunks:
            async with self.pool.acquire() as conn:
                result = await export_chunk(
                    conn=conn,
                    chunk=chunk,
                    archive_path=self.archive_path,
                    compression=self.config.archive_compression,
                )

                # Record in archival_log regardless of success/empty
                # This prevents re-discovering the same chunk repeatedly
                if result.success:
                    await record_archival(conn, result, chunk)

            results.append(result)

        # ── Build summary ────────────────────────────────────
        successful = [r for r in results if r.success]
        failed = [r for r in results if not r.success]
        skipped = [r for r in successful if r.record_count == 0]
        written = [r for r in successful if r.record_count > 0]

        total_records = sum(r.record_count for r in written)
        total_bytes = sum(r.file_size_bytes for r in written)
        duration_ms = time.time() * 1000 - pass_start_ms

        summary = ArchivalSummary(
            chunks_found=len(chunks),
            chunks_succeeded=len(successful),
            chunks_skipped=len(skipped),
            chunks_failed=len(failed),
            total_records=total_records,
            total_bytes=total_bytes,
            duration_ms=duration_ms,
            errors=[r.error for r in failed if r.error],
        )

        logger.info(
            f"Archival pass complete: "
            f"{len(written)} files written, "
            f"{total_records:,} records, "
            f"{total_bytes / 1_048_576:.1f} MB, "
            f"{duration_ms:.0f}ms"
        )

        if failed:
            logger.error(f"{len(failed)} chunk(s) failed: {[r.error for r in failed]}")

        return summary

    async def run_scheduled(
        self,
        hour: int = 2,
        minute: int = 0,
    ) -> None:
        """
        Run the archival job on a nightly schedule.

        Calculates the next run time, sleeps until then,
        runs one pass, and repeats. Errors in a single pass
        are logged but do not stop future scheduled runs.

        Args:
            hour:   Local hour to run (0–23). Default 2am.
            minute: Local minute to run (0–59). Default 0.
        """
        logger.info(
            f"Archival scheduler started — "
            f"running nightly at {hour:02d}:{minute:02d} local time"
        )

        while True:
            now = datetime.now()
            next_run = now.replace(
                hour=hour,
                minute=minute,
                second=0,
                microsecond=0,
            )

            # If the scheduled time has already passed today, run tomorrow
            if next_run <= now:
                next_run += timedelta(days=1)

            wait_seconds = (next_run - now).total_seconds()
            logger.info(
                f"Next archival run: "
                f"{next_run.strftime('%Y-%m-%d %H:%M:%S')} "
                f"({wait_seconds / 3600:.1f}h from now)"
            )

            await asyncio.sleep(wait_seconds)

            try:
                await self.run_once()
            except Exception as e:
                # Log and continue scheduling — never let one failure
                # stop future nightly runs
                logger.error(
                    f"Archival pass raised unexpected exception: {e}",
                    exc_info=True,
                )


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────


async def _main(schedule: bool, run_once: bool) -> None:
    """
    Load config from environment and run the archival job.
    Called by __main__ block below.
    """
    import os

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s — %(message)s",
    )

    config = StorageConfig(
        hot_retention_days=int(os.environ.get("HOT_RETENTION_DAYS", 90)),
        compress_after_days=int(os.environ.get("COMPRESS_AFTER_DAYS", 7)),
        archive_enabled=os.environ.get("ARCHIVE_ENABLED", "true").lower() == "true",
        archive_after_days=int(os.environ.get("ARCHIVE_AFTER_DAYS", 85)),
        archive_path=os.environ.get("ARCHIVE_PATH", "/var/lib/acropora/archive"),
        archive_compression=os.environ.get("ARCHIVE_COMPRESSION", "zstd"),
        archive_target_file_size_mb=int(
            os.environ.get("ARCHIVE_TARGET_FILE_SIZE_MB", 256)
        ),
    )

    database_url = os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(database_url, min_size=1, max_size=3)

    try:
        job = ArchivalJob(pool=pool, config=config)

        if schedule:
            archive_hour = int(os.environ.get("ARCHIVE_HOUR", 2))
            archive_minute = int(os.environ.get("ARCHIVE_MINUTE", 0))
            await job.run_scheduled(hour=archive_hour, minute=archive_minute)
        else:
            # --run-once or no flag — run once and exit
            summary = await job.run_once()
            if summary.chunks_failed > 0:
                raise SystemExit(1)
    finally:
        await pool.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Acropora Parquet archival pipeline")
    group = parser.add_mutually_exclusive_group()
    group.add_argument(
        "--schedule",
        action="store_true",
        help="Run on nightly schedule (default: run once and exit)",
    )
    group.add_argument(
        "--run-once",
        action="store_true",
        help="Run one archival pass immediately and exit",
    )
    args = parser.parse_args()

    asyncio.run(_main(schedule=args.schedule, run_once=args.run_once))
