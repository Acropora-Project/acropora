"""
acropora_store/query_router.py

Tiered query router.

Routes queries to the appropriate storage tier:
- Hot tier (TimescaleDB): recent data within hot_retention_days
- Cold tier (DuckDB + Parquet): historical data beyond hot tier
- Both tiers: queries spanning the hot/cold boundary

From the caller's perspective this is transparent — one query,
one result, correct data regardless of which tier(s) it came from.
"""

from __future__ import annotations

import logging
import time

from acropora_store.duckdb_query import DuckDBQueryEngine
from acropora_store.executor import QueryExecutor
from acropora_store.query_engine import ObservationQuery, QueryResponse, TimeRange
from acropora_store.schema import StorageConfig

logger = logging.getLogger(__name__)


class TieredQueryRouter:
    """
    Routes observation queries across hot (TimescaleDB) and
    cold (DuckDB + Parquet) storage tiers transparently.
    """

    def __init__(
        self,
        hot_executor: QueryExecutor,
        cold_executor: DuckDBQueryEngine,  # type: ignore[name-defined]
        config: StorageConfig,
    ) -> None:
        self.hot = hot_executor
        self.cold = cold_executor
        self.config = config

    def _hot_tier_boundary_us(self) -> int:
        """Timestamp before which data may have been archived to Parquet."""
        return int((time.time() - self.config.hot_retention_days * 86400) * 1_000_000)

    async def execute(self, query: ObservationQuery) -> QueryResponse:
        """
        Execute an observation query across storage tiers.
        Merges results from hot and cold tiers if query spans both.
        """
        boundary_us = self._hot_tier_boundary_us()
        query_start = query.time_range.start_us
        query_end = query.time_range.end_us

        needs_hot = query_end > boundary_us
        needs_cold = query_start < boundary_us and self.config.archive_enabled

        hot_records: list = []
        cold_records: list = []
        start_ms = time.time() * 1000

        if needs_hot:
            hot_query = ObservationQuery(
                time_range=TimeRange(
                    start_us=max(query_start, boundary_us),
                    end_us=query_end,
                ),
                spatial_filter=query.spatial_filter,
                payload_type=query.payload_type,
                source_id=query.source_id,
                altitude_min_m=query.altitude_min_m,
                altitude_max_m=query.altitude_max_m,
                limit=query.limit,
                offset=query.offset,
                order=query.order,
            )
            hot_response = await self.hot.execute_observation_query(hot_query)
            hot_records = hot_response.records

        if needs_cold:
            cold_query = ObservationQuery(
                time_range=TimeRange(
                    start_us=query_start,
                    end_us=min(query_end, boundary_us),
                ),
                spatial_filter=query.spatial_filter,
                payload_type=query.payload_type,
                source_id=query.source_id,
                altitude_min_m=query.altitude_min_m,
                altitude_max_m=query.altitude_max_m,
                limit=query.limit,
                order=query.order,
            )
            cold_records = self.cold.query(cold_query)

        # Merge and sort
        all_records = hot_records + cold_records
        all_records.sort(
            key=lambda r: r.timestamp_us,
            reverse=(query.order == "desc"),
        )
        final_records = all_records[: query.limit]

        query_time_ms = time.time() * 1000 - start_ms

        # Build tier note for consumers
        tiers_used = []
        if needs_hot and hot_records:
            tiers_used.append("hot (TimescaleDB)")
        if needs_cold and cold_records:
            tiers_used.append("cold (Parquet)")

        coverage_note = f"Served from: {', '.join(tiers_used)}" if tiers_used else None

        return QueryResponse(
            records=final_records,
            total_count=len(all_records),
            returned_count=len(final_records),
            query_time_ms=query_time_ms,
            node_id=self.hot.node_id,
            served_at_us=time.time_ns() // 1000,
            coverage_note=coverage_note,
        )
