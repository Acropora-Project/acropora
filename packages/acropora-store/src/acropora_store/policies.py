"""
acropora_store/policies.py

Idempotent application of TimescaleDB storage policies from operator config.

Called at node startup and whenever StorageConfig changes.
All policy operations are safe to call multiple times —
existing policies are removed and re-applied with new values.
"""

from __future__ import annotations

import logging

import asyncpg

from acropora_store.schema import StorageConfig

logger = logging.getLogger(__name__)


async def apply_storage_policies(
    conn: asyncpg.Connection,
    config: StorageConfig,
) -> None:
    """
    Apply storage policies from StorageConfig to TimescaleDB.
    Idempotent — safe to call on every startup.

    Applies:
    - Compression policy on observations table
    - Retention policy on observations table
    - Retention policies on continuous aggregates (if enabled)
    """
    logger.info("Applying storage policies from config")

    await _apply_compression_policy(conn, config)
    await _apply_retention_policy(conn, config)

    if config.aggregates_enabled:
        await _apply_aggregate_retention_policies(conn, config)

    logger.info("Storage policies applied")


async def _apply_compression_policy(
    conn: asyncpg.Connection,
    config: StorageConfig,
) -> None:
    """
    Apply compression policy to the observations hypertable.
    Compresses chunks older than compress_after_days.
    """
    # Remove existing policy before re-applying
    await conn.execute(
        "SELECT remove_compression_policy('observations', if_exists => true)"
    )

    if config.compress_after_days <= 0:
        logger.info("Compression policy disabled (compress_after_days=0)")
        return

    await conn.execute(
        """
        SELECT add_compression_policy(
            'observations',
            compress_after => $1::interval,
            if_not_exists => true
        )
        """,
        f"{config.compress_after_days} days",
    )
    logger.info(
        f"Compression policy: compress chunks older than "
        f"{config.compress_after_days} days"
    )


async def _apply_retention_policy(
    conn: asyncpg.Connection,
    config: StorageConfig,
) -> None:
    """
    Apply retention policy to the observations hypertable.
    Drops chunks older than hot_retention_days.

    Note: Parquet archival job must run before this drops chunks.
    The archival job runs at archive_after_days which must be
    less than hot_retention_days.
    """
    await conn.execute(
        "SELECT remove_retention_policy('observations', if_exists => true)"
    )

    if config.hot_retention_days <= 0:
        logger.info("Retention policy disabled (hot_retention_days=0 — keep forever)")
        return

    await conn.execute(
        """
        SELECT add_retention_policy(
            'observations',
            drop_after => $1::interval,
            if_not_exists => true
        )
        """,
        f"{config.hot_retention_days} days",
    )
    logger.info(
        f"Retention policy: drop chunks older than {config.hot_retention_days} days"
    )


async def _apply_aggregate_retention_policies(
    conn: asyncpg.Connection,
    config: StorageConfig,
) -> None:
    """
    Apply retention policies to continuous aggregates.
    Only called if aggregates_enabled = True.
    """
    # Hourly density aggregate — keep 1 year by default
    hourly_views = [
        "obs_density_1h_01deg",
        "obs_source_tracks_1h",
    ]
    for view in hourly_views:
        # Check if view exists before applying policy
        exists = await conn.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = $1)",
            view,
        )
        if not exists:
            continue

        await conn.execute(
            f"SELECT remove_retention_policy('{view}', if_exists => true)"
        )
        await conn.execute(
            f"""
            SELECT add_retention_policy(
                '{view}',
                drop_after => '365 days'::interval,
                if_not_exists => true
            )
            """
        )

    # Daily density aggregate — keep forever (tiny storage footprint)
    daily_view = "obs_density_1d_01deg"
    exists = await conn.fetchval(
        "SELECT EXISTS (SELECT 1 FROM pg_matviews WHERE matviewname = $1)",
        daily_view,
    )
    if exists:
        await conn.execute(
            f"SELECT remove_retention_policy('{daily_view}', if_exists => true)"
        )
        # No retention policy = keep forever


async def apply_schema_migration(conn: asyncpg.Connection) -> None:
    """
    Apply the observations hypertable schema if it doesn't exist.
    Called once on first startup.

    Requires TimescaleDB and PostGIS extensions to be installed.
    In acropora-compose these are installed via the init.sql file.
    """
    # Create observations table
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS observations (
            timestamp_us            BIGINT              NOT NULL,
            lat                     DOUBLE PRECISION    NOT NULL,
            lon                     DOUBLE PRECISION    NOT NULL,
            altitude_m              REAL,
            altitude_type           TEXT,
            source_id               TEXT,

            record_id               CHAR(64)            NOT NULL,
            node_id                 TEXT                NOT NULL,
            payload_type            TEXT                NOT NULL,
            payload_schema          TEXT                NOT NULL,
            payload_schema_version  TEXT                NOT NULL,
            ingested_at_us          BIGINT              NOT NULL,
            signed_at_us            BIGINT              NOT NULL,
            signature               TEXT                NOT NULL,

            payload                 JSONB               NOT NULL DEFAULT '{}',
            tags                    JSONB,

            geom                    GEOMETRY(Point, 4326)
                                    GENERATED ALWAYS AS (
                                        ST_SetSRID(ST_MakePoint(lon, lat), 4326)
                                    ) STORED,

            CONSTRAINT valid_lat
                CHECK (lat BETWEEN -90 AND 90),
            CONSTRAINT valid_lon
                CHECK (lon BETWEEN -180 AND 180),
            CONSTRAINT valid_record_id
                CHECK (length(record_id) = 64),
            CONSTRAINT valid_payload_type
                CHECK (payload_type IN (
                    'adsb', 'ais', 'aprs', 'weather', 'seismic', 'custom'
                ))
        )
    """)

    # Convert to hypertable if not already
    await conn.execute("""
        SELECT create_hypertable(
            'observations',
            'timestamp_us',
            chunk_time_interval => 86400000000,
            if_not_exists => TRUE
        )
    """)

    # Set compression settings
    await conn.execute("""
        ALTER TABLE observations SET (
            timescaledb.compress,
            timescaledb.compress_segmentby = 'source_id, payload_type',
            timescaledb.compress_orderby = 'timestamp_us ASC'
        )
    """)

    # Indexes
    await conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_obs_record_id "
        "ON observations (record_id)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_time_desc "
        "ON observations (timestamp_us DESC)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_type_time "
        "ON observations (payload_type, timestamp_us DESC)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_source_time "
        "ON observations (source_id, timestamp_us DESC) "
        "WHERE source_id IS NOT NULL"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_node_time "
        "ON observations (node_id, timestamp_us DESC)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_geom ON observations USING GIST (geom)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_adsb_spatial "
        "ON observations (timestamp_us DESC, lat, lon) "
        "WHERE payload_type = 'adsb'"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_tags "
        "ON observations USING GIN (tags) "
        "WHERE tags IS NOT NULL"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_obs_payload "
        "ON observations USING GIN (payload jsonb_path_ops)"
    )

    # Supporting tables
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS node_config (
            node_id                 TEXT            NOT NULL PRIMARY KEY,
            public_key              TEXT            NOT NULL,
            created_at_us           BIGINT          NOT NULL,
            protocol_version        TEXT            NOT NULL DEFAULT '0.1.0',
            privacy_config_json     JSONB           NOT NULL DEFAULT '{}',
            storage_config_json     JSONB           NOT NULL DEFAULT '{}',
            query_policy_json       JSONB           NOT NULL DEFAULT '{}',
            updated_at_us           BIGINT          NOT NULL
        )
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS ingest_stats (
            day                     DATE            NOT NULL,
            payload_type            TEXT            NOT NULL,
            records_ingested        BIGINT          NOT NULL DEFAULT 0,
            records_invalid         BIGINT          NOT NULL DEFAULT 0,
            unique_sources          INTEGER         NOT NULL DEFAULT 0,
            bytes_written           BIGINT          NOT NULL DEFAULT 0,
            updated_at_us           BIGINT          NOT NULL,
            PRIMARY KEY (day, payload_type)
        )
    """)

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS dead_letter (
            id                      BIGSERIAL       PRIMARY KEY,
            captured_at_us          BIGINT          NOT NULL,
            source_type             TEXT            NOT NULL,
            raw_fields              JSONB           NOT NULL,
            errors                  TEXT[]          NOT NULL,
            created_at_us           BIGINT          NOT NULL
                DEFAULT (EXTRACT(EPOCH FROM now()) * 1000000)::BIGINT
        )
    """)
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_dead_letter_time "
        "ON dead_letter (captured_at_us DESC)"
    )

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS archival_log (
            chunk_id                BIGINT          NOT NULL PRIMARY KEY,
            chunk_start_us          BIGINT          NOT NULL,
            chunk_end_us            BIGINT          NOT NULL,
            payload_type            TEXT            NOT NULL,
            record_count            BIGINT          NOT NULL,
            parquet_path            TEXT            NOT NULL,
            parquet_size_bytes      BIGINT          NOT NULL,
            archived_at_us          BIGINT          NOT NULL,
            checksum_sha256         CHAR(64)        NOT NULL
        )
    """)
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_archival_log_time "
        "ON archival_log (chunk_start_us DESC)"
    )

    await conn.execute("""
        CREATE TABLE IF NOT EXISTS track_sightings (
            id                      BIGSERIAL       PRIMARY KEY,
            source_id               TEXT            NOT NULL,
            payload_type            TEXT            NOT NULL,
            node_id                 TEXT            NOT NULL,
            bbox_lat_min            DOUBLE PRECISION NOT NULL,
            bbox_lon_min            DOUBLE PRECISION NOT NULL,
            bbox_lat_max            DOUBLE PRECISION NOT NULL,
            bbox_lon_max            DOUBLE PRECISION NOT NULL,
            first_seen_us           BIGINT          NOT NULL,
            last_seen_us            BIGINT          NOT NULL,
            observation_count       INTEGER         NOT NULL,
            signed_at_us            BIGINT          NOT NULL,
            signature               TEXT            NOT NULL,
            published_at_us         BIGINT,
            CONSTRAINT valid_sighting_type CHECK (payload_type IN (
                'adsb', 'ais', 'aprs', 'weather', 'seismic', 'custom'
            ))
        )
    """)
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_track_source_time "
        "ON track_sightings (source_id, first_seen_us DESC)"
    )
    await conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_track_unpublished "
        "ON track_sightings (published_at_us) "
        "WHERE published_at_us IS NULL"
    )
