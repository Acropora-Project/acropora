"""
acropora_store/duckdb_query.py

DuckDB query engine for the Parquet cold storage tier.

Queries Parquet archive files using DuckDB. Used by TieredQueryRouter
when the requested time range extends beyond the TimescaleDB hot tier.

Each DuckDBQueryEngine instance owns its own DuckDB connection —
DuckDB connections are not thread-safe and must not be shared.
The TieredQueryRouter creates one instance per query or uses a
connection-per-thread model for concurrent access.

Architecture note:
    Parquet files are organized on disk as:
        {archive_path}/{payload_type}/{YYYY}/{MM}/{YYYY-MM-DD}.parquet

    This directory partitioning lets DuckDB skip entire directories
    when time range and payload_type predicates don't match —
    critical for fast historical queries.
"""

from __future__ import annotations

import glob
import json
import logging
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from acropora_store.privacy import ServedRecord, haversine_km, serve_record
from acropora_store.query_engine import (
    BoundingBox,
    LocationRadius,
    ObservationQuery,
    SpatialFilter,
    TimeRange,
)
from acropora_store.schema import PrivacyConfig

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Archive statistics
# ─────────────────────────────────────────────


@dataclass
class ArchiveStats:
    """Summary statistics about the Parquet archive."""

    file_count: int
    total_size_bytes: int
    payload_types: list[dict]  # Per-type summary dicts


# ─────────────────────────────────────────────
# DuckDB query engine
# ─────────────────────────────────────────────


class DuckDBQueryEngine:
    """
    Queries Parquet archive files using DuckDB.

    Used for historical queries beyond the TimescaleDB hot tier.
    Each instance owns one DuckDB in-memory connection.

    Not thread-safe — create one instance per thread or use
    a connection pool pattern.
    """

    def __init__(
        self,
        archive_path: str,
        privacy_config: PrivacyConfig,
        node_id: str,
        threads: int = 4,
        memory_limit: str = "1GB",
    ) -> None:
        self.archive_path = Path(archive_path)
        self.privacy_config = privacy_config
        self.node_id = node_id

        # Each instance owns its own in-memory DuckDB connection
        # DuckDB connections are not thread-safe — never share this
        self._conn = duckdb.connect(database=":memory:")
        self._conn.execute(f"SET threads = {threads}")
        self._conn.execute(f"SET memory_limit = '{memory_limit}'")

        logger.debug(
            f"DuckDBQueryEngine initialized: archive={archive_path}, "
            f"threads={threads}, memory_limit={memory_limit}"
        )

    def close(self) -> None:
        """Explicitly close the DuckDB connection."""
        self._conn.close()

    def __enter__(self) -> DuckDBQueryEngine:
        return self

    def __exit__(self, *args: object) -> None:
        self.close()

    # ─────────────────────────────────────────
    # Glob pattern building
    # ─────────────────────────────────────────

    def _build_parquet_glob(
        self,
        time_range: TimeRange,
        payload_type: str | None,
    ) -> str:
        """
        Build a glob pattern for Parquet files that could contain
        data matching this query's time range and payload type.

        Uses directory partitioning to skip irrelevant files.
        More specific patterns = faster DuckDB planning.

        Pattern structure:
            {archive_path}/{payload_type}/{YYYY}/{MM}/*.parquet

        Examples:
            Single month:  .../adsb/2026/01/*.parquet
            Full year:     .../adsb/2026/**/*.parquet
            Multi-year:    .../adsb/**/*.parquet
            All types:     .../**/*.parquet
        """
        start_dt = datetime.fromtimestamp(
            time_range.start_us / 1_000_000,
            tz=UTC,
        )
        end_dt = datetime.fromtimestamp(
            time_range.end_us / 1_000_000,
            tz=UTC,
        )

        type_segment = payload_type if payload_type is not None else "**"

        # Same year and month — most specific pattern
        if start_dt.year == end_dt.year and start_dt.month == end_dt.month:
            return str(
                self.archive_path
                / type_segment
                / start_dt.strftime("%Y")
                / start_dt.strftime("%m")
                / "*.parquet"
            )

        # Same year, different months
        if start_dt.year == end_dt.year:
            return str(
                self.archive_path
                / type_segment
                / start_dt.strftime("%Y")
                / "**"
                / "*.parquet"
            )

        # Spans multiple years — widest pattern
        return str(self.archive_path / type_segment / "**" / "*.parquet")

    def _matching_files(
        self,
        time_range: TimeRange,
        payload_type: str | None,
    ) -> list[str]:
        """
        Return list of Parquet files matching the glob pattern.
        Returns empty list if no files exist — never raises.
        """
        pattern = self._build_parquet_glob(time_range, payload_type)
        # Use forward slashes for DuckDB on Windows
        matches = glob.glob(pattern, recursive=True)
        return matches

    # ─────────────────────────────────────────
    # SQL building
    # ─────────────────────────────────────────

    def _build_spatial_condition(
        self,
        spatial_filter: SpatialFilter,
        params: list,
    ) -> str:
        """
        Build the WHERE clause fragment for a spatial filter.
        Appends required parameter values to params in place.

        For LocationRadius, uses a bounding box pre-filter in SQL
        then applies precise Haversine filtering in Python afterward.
        The SQL pre-filter eliminates the bulk of rows; the Python
        post-filter ensures exact circle boundary correctness.
        """
        if isinstance(spatial_filter, BoundingBox):
            params.extend(
                [
                    spatial_filter.lat_min,
                    spatial_filter.lat_max,
                    spatial_filter.lon_min,
                    spatial_filter.lon_max,
                ]
            )
            return "lat >= ? AND lat <= ? AND lon >= ? AND lon <= ?"

        elif isinstance(spatial_filter, LocationRadius):
            # Use the bounding box approximation for SQL pre-filter
            bbox = spatial_filter.to_bbox()
            params.extend(
                [
                    bbox.lat_min,
                    bbox.lat_max,
                    bbox.lon_min,
                    bbox.lon_max,
                ]
            )
            return "lat >= ? AND lat <= ? AND lon >= ? AND lon <= ?"

        else:
            raise TypeError(f"Unknown spatial filter type: {type(spatial_filter)}")

    def _build_query_sql(
        self,
        parquet_glob: str,
        query: ObservationQuery,
    ) -> tuple[str, list]:
        """
        Build the full SELECT SQL and params list for a Parquet query.
        Returns (sql, params).
        """
        params: list = []

        # Time range — always present
        time_conditions = "timestamp_us >= ? AND timestamp_us < ?"
        params.extend([query.time_range.start_us, query.time_range.end_us])

        # Spatial — always present
        spatial_condition = self._build_spatial_condition(query.spatial_filter, params)

        conditions = [time_conditions, spatial_condition]

        # Optional filters
        if query.payload_type is not None:
            conditions.append("payload_type = ?")
            params.append(query.payload_type)

        if query.source_id is not None:
            conditions.append("source_id = ?")
            params.append(query.source_id)

        if query.altitude_min_m is not None:
            conditions.append("altitude_m >= ?")
            params.append(query.altitude_min_m)

        if query.altitude_max_m is not None:
            conditions.append("altitude_m <= ?")
            params.append(query.altitude_max_m)

        where_clause = " AND ".join(conditions)
        order_dir = "DESC" if query.order == "desc" else "ASC"

        # Use forward slashes in the glob path — DuckDB requires this on Windows
        safe_glob = parquet_glob.replace("\\", "/")

        sql = f"""
            SELECT
                timestamp_us, lat, lon, altitude_m, altitude_type, source_id,
                record_id, node_id, payload_type, payload_schema,
                payload_schema_version, signed_at_us, signature,
                tags
            FROM read_parquet('{safe_glob}', hive_partitioning = false)
            WHERE {where_clause}
            ORDER BY timestamp_us {order_dir}
            LIMIT ?
        """
        params.append(query.limit)
        return sql, params

    # ─────────────────────────────────────────
    # Payload reconstruction
    # ─────────────────────────────────────────

    def _reconstruct_payload(
        self,
        row: dict,
        payload_type: str,
    ) -> dict:
        """
        Reconstruct the payload dict from Parquet columns.

        At archival time, ADS-B payload fields are flattened into
        top-level columns for columnar compression efficiency.
        Here we reassemble them into the canonical payload dict.

        Generic payload types were stored as a JSON string in the
        'payload_json' column (future implementation).
        """
        if payload_type == "adsb":
            return {
                "callsign": row.get("callsign"),
                "squawk": row.get("squawk"),
                "altitude_baro_ft": row.get("altitude_baro_ft"),
                "ground_speed_kt": row.get("ground_speed_kt"),
                "track_deg": row.get("track_deg"),
                "vertical_rate_fpm": row.get("vertical_rate_fpm"),
                "nic": row.get("nic"),
                "nac_p": row.get("nac_p"),
                "category": row.get("category"),
                "rssi_dbm": row.get("rssi_dbm"),
                "messages_seen": row.get("messages_seen"),
            }
        else:
            # Generic payload types — stored as JSON string
            payload_json = row.get("payload_json") or "{}"
            try:
                return json.loads(payload_json)
            except (json.JSONDecodeError, TypeError):
                return {}

    # ─────────────────────────────────────────
    # Precise radius filter
    # ─────────────────────────────────────────

    @staticmethod
    def _within_radius(
        lat: float,
        lon: float,
        center_lat: float,
        center_lon: float,
        radius_km: float,
    ) -> bool:
        """
        Haversine distance check — used as a post-filter after the
        SQL bounding box pre-filter for LocationRadius queries.
        Returns True if (lat, lon) is within radius_km of center.
        """
        return haversine_km(center_lat, center_lon, lat, lon) <= radius_km

    # ─────────────────────────────────────────
    # Main query entry point
    # ─────────────────────────────────────────

    def query(self, query: ObservationQuery) -> list[ServedRecord]:
        """
        Execute an observation query against the Parquet archive.

        Returns served records with privacy transformations applied.
        Returns an empty list if no matching Parquet files exist —
        never raises on missing files.

        Thread safety: do NOT call this from multiple threads on the
        same DuckDBQueryEngine instance. Create separate instances
        per thread instead.
        """
        start_ms = time.time() * 1000

        # Find matching Parquet files before building SQL
        matching_files = self._matching_files(
            query.time_range,
            query.payload_type,
        )

        if not matching_files:
            logger.debug(
                f"DuckDB query: no Parquet files found for "
                f"payload_type={query.payload_type}, "
                f"time_range={query.time_range}"
            )
            return []

        parquet_glob = self._build_parquet_glob(
            query.time_range,
            query.payload_type,
        )

        sql, params = self._build_query_sql(parquet_glob, query)

        try:
            self._conn.execute(sql, params)
            raw_rows = self._conn.fetchall()
        except duckdb.IOException as e:
            # Handles race condition where file disappears between
            # glob scan and query execution
            if "No files found" in str(e) or "file not found" in str(e).lower():
                logger.debug(f"DuckDB query: file disappeared during query: {e}")
                return []
            raise
        except duckdb.Error as e:
            logger.error(f"DuckDB query error: {e}", exc_info=True)
            raise

        # Map column positions to names
        # Must match SELECT column order in _build_query_sql
        columns = [
            "timestamp_us",
            "lat",
            "lon",
            "altitude_m",
            "altitude_type",
            "source_id",
            "record_id",
            "node_id",
            "payload_type",
            "payload_schema",
            "payload_schema_version",
            "signed_at_us",
            "signature",
            "tags",
        ]

        served_records: list[ServedRecord] = []
        radius_filter = (
            query.spatial_filter
            if isinstance(query.spatial_filter, LocationRadius)
            else None
        )

        for raw_row in raw_rows:
            row = dict(zip(columns, raw_row))

            # Precise radius post-filter for LocationRadius queries
            # (SQL used bounding box approximation — this is the exact check)
            if radius_filter is not None:
                if not self._within_radius(
                    lat=row["lat"],
                    lon=row["lon"],
                    center_lat=radius_filter.lat,
                    center_lon=radius_filter.lon,
                    radius_km=radius_filter.radius_km,
                ):
                    continue

            # Reconstruct payload from flattened Parquet columns
            row["payload"] = self._reconstruct_payload(row, row["payload_type"])

            # Parse tags from JSON string if present
            tags_raw = row.get("tags")
            if isinstance(tags_raw, str) and tags_raw:
                try:
                    row["tags"] = json.loads(tags_raw)
                except (json.JSONDecodeError, TypeError):
                    row["tags"] = None
            elif not isinstance(tags_raw, list):
                row["tags"] = None

            # Apply privacy transformations
            served = serve_record(row, self.privacy_config, self.node_id)
            if served is not None:
                served_records.append(served)

        duration_ms = time.time() * 1000 - start_ms
        logger.debug(
            f"DuckDB query: {len(served_records)} served records, "
            f"{len(raw_rows)} raw rows, {len(matching_files)} files, "
            f"{duration_ms:.1f}ms"
        )

        return served_records

    # ─────────────────────────────────────────
    # Archive statistics
    # ─────────────────────────────────────────

    def get_archive_stats(self) -> ArchiveStats:
        """
        Summary statistics about the Parquet archive.
        Used by acropora-dashboard and the /coverage endpoint.

        Scans all Parquet files — may be slow on very large archives.
        Results should be cached by the caller.
        """
        all_files = glob.glob(
            str(self.archive_path / "**" / "*.parquet"),
            recursive=True,
        )

        if not all_files:
            return ArchiveStats(
                file_count=0,
                total_size_bytes=0,
                payload_types=[],
            )

        total_size = sum(Path(f).stat().st_size for f in all_files)

        # Use forward slashes for DuckDB glob
        safe_glob = str(self.archive_path / "**" / "*.parquet").replace("\\", "/")

        try:
            self._conn.execute(f"""
                SELECT
                    payload_type,
                    count(*)            AS record_count,
                    min(timestamp_us)   AS earliest_us,
                    max(timestamp_us)   AS latest_us
                FROM read_parquet('{safe_glob}', hive_partitioning = false)
                GROUP BY payload_type
                ORDER BY payload_type
            """)
            rows = self._conn.fetchall()

            payload_types = [
                {
                    "payload_type": row[0],
                    "record_count": row[1],
                    "earliest_us": row[2],
                    "latest_us": row[3],
                }
                for row in rows
            ]
        except duckdb.Error as e:
            logger.warning(f"Could not compute archive stats: {e}")
            payload_types = []

        return ArchiveStats(
            file_count=len(all_files),
            total_size_bytes=total_size,
            payload_types=payload_types,
        )
