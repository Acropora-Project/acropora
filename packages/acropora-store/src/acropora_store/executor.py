"""
acropora_store/executor.py

Async query executor for TimescaleDB.

Applies query policy enforcement before execution and privacy
transformations after — so consumers always receive served records,
never raw stored records.
"""

from __future__ import annotations

import logging
import time

import asyncpg

from acropora_store.privacy import serve_record
from acropora_store.query_engine import (
    CoverageQuery,
    CoverageResponse,
    ObservationQuery,
    QueryResponse,
    SourceQuery,
)
from acropora_store.query_policy import (
    enforce_observation_query_policy,
    enforce_source_query_policy,
)
from acropora_store.schema import PrivacyConfig, QueryPolicyConfig
from acropora_store.sql_builder import (
    build_count_query,
    build_coverage_query,
    build_observation_query,
    build_source_query,
)

logger = logging.getLogger(__name__)


class QueryExecutor:
    """
    Executes Reef Protocol observation queries against TimescaleDB.

    Responsibilities:
    - Query policy enforcement (raises QueryPolicyViolation on violations)
    - SQL construction via sql_builder
    - Async execution against TimescaleDB connection pool
    - Privacy transformation via privacy.serve_record()
    - QueryResponse construction
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        privacy_config: PrivacyConfig,
        query_policy: QueryPolicyConfig,
        node_id: str,
    ) -> None:
        self.pool = pool
        self.privacy_config = privacy_config
        self.query_policy = query_policy
        self.node_id = node_id

    async def execute_observation_query(
        self,
        query: ObservationQuery,
    ) -> QueryResponse:
        """
        Execute an observation query.
        Enforces policy, runs SQL, applies privacy, returns QueryResponse.
        Raises QueryPolicyViolation if any policy limit is exceeded.
        """
        start_ms = time.time() * 1000

        # Enforce query policy — raises QueryPolicyViolation if violated
        enforce_observation_query_policy(query, self.query_policy)

        sql, params = build_observation_query(query)
        count_sql, count_params = build_count_query(query)

        async with self.pool.acquire() as conn:
            import asyncio

            rows, count_row = await asyncio.gather(
                conn.fetch(sql, *params),
                conn.fetchrow(count_sql, *count_params),
            )

        total_count = count_row["count"] if count_row else 0

        # Apply privacy transformations
        served = []
        filtered_count = 0
        for row in rows:
            record = serve_record(dict(row), self.privacy_config, self.node_id)
            if record is None:
                filtered_count += 1
            else:
                served.append(record)

        query_time_ms = time.time() * 1000 - start_ms

        coverage_note = None
        if filtered_count > 0:
            coverage_note = (
                f"{filtered_count} record(s) withheld by privacy policy "
                f"(exclusion zone or embargo)"
            )

        logger.debug(
            f"ObservationQuery: {len(served)} served, {filtered_count} filtered, "
            f"{query_time_ms:.1f}ms"
        )

        return QueryResponse(
            records=served,
            total_count=total_count,
            returned_count=len(served),
            query_time_ms=query_time_ms,
            node_id=self.node_id,
            served_at_us=time.time_ns() // 1000,
            coverage_note=coverage_note,
        )

    async def execute_source_query(
        self,
        query: SourceQuery,
    ) -> QueryResponse:
        """
        Execute a source track query.
        Returns all observations for a specific source_id within scope.
        """
        start_ms = time.time() * 1000

        enforce_source_query_policy(query, self.query_policy)

        sql, params = build_source_query(query)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        served = []
        filtered_count = 0
        for row in rows:
            record = serve_record(dict(row), self.privacy_config, self.node_id)
            if record is None:
                filtered_count += 1
            else:
                served.append(record)

        query_time_ms = time.time() * 1000 - start_ms

        coverage_note = None
        if filtered_count > 0:
            coverage_note = f"{filtered_count} record(s) withheld by privacy policy"

        return QueryResponse(
            records=served,
            total_count=len(rows),
            returned_count=len(served),
            query_time_ms=query_time_ms,
            node_id=self.node_id,
            served_at_us=time.time_ns() // 1000,
            coverage_note=coverage_note,
        )

    async def execute_coverage_query(
        self,
        query: CoverageQuery,
    ) -> CoverageResponse:
        """
        Execute a coverage summary query.
        Returns statistics about what data this node holds.
        """
        sql, params = build_coverage_query(query)

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(sql, *params)

        payload_types = [
            {
                "payload_type": row["payload_type"],
                "record_count": row["record_count"],
                "unique_sources": row["unique_sources"],
                "earliest_us": row["earliest_us"],
                "latest_us": row["latest_us"],
                "bbox": [
                    row["lat_min"],
                    row["lon_min"],
                    row["lat_max"],
                    row["lon_max"],
                ],
                "alt_range_m": [row["alt_min_m"], row["alt_max_m"]],
            }
            for row in rows
        ]

        return CoverageResponse(
            node_id=self.node_id,
            payload_types=payload_types,
            generated_at_us=time.time_ns() // 1000,
        )
