"""
acropora_store/schema.py

Field constants, configuration dataclasses, and Parquet schema definitions.
Single source of truth for the canonical observation record schema.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import pyarrow as pa

# ─────────────────────────────────────────────
# Controlled vocabularies
# ─────────────────────────────────────────────

PAYLOAD_TYPES: Final = frozenset(
    {
        "adsb",
        "ais",
        "aprs",
        "weather",
        "seismic",
        "custom",
    }
)

ALTITUDE_PRECISIONS: Final = frozenset(
    {
        "10000ft",
        "1000ft",
        "100ft",
        "exact",
        "stripped",
    }
)

RSSI_MODES: Final = frozenset(
    {
        "strip",
        "bucketed",
        "quantized",
        "full",
    }
)

ACCESS_MODES: Final = frozenset(
    {
        "public",
        "key_required",
        "allowlist",
        "private",
    }
)

# ─────────────────────────────────────────────
# Storage configuration
# ─────────────────────────────────────────────


@dataclass(frozen=True)
class StorageConfig:
    """
    Operator-configurable storage policy settings.
    Loaded from node_config table at startup.
    Applied to TimescaleDB policies via policies.py.
    """

    # Hot tier — TimescaleDB
    hot_retention_days: int = 90
    compress_after_days: int = 7

    # Archival — Parquet
    archive_enabled: bool = True
    archive_after_days: int = 85
    archive_path: str = "/var/lib/acropora/archive"
    archive_compression: str = "zstd"
    archive_target_file_size_mb: int = 256

    # Optional continuous aggregates — disabled by default
    # Operators opt in for local analytics / dashboard
    aggregates_enabled: bool = False
    hourly_density_enabled: bool = False
    daily_density_enabled: bool = False
    track_summary_enabled: bool = False
    expose_density_api: bool = False

    def __post_init__(self) -> None:
        if self.hot_retention_days > 0:
            if self.compress_after_days >= self.hot_retention_days:
                raise ValueError(
                    f"compress_after_days ({self.compress_after_days}) must be "
                    f"less than hot_retention_days ({self.hot_retention_days})"
                )
            if (
                self.archive_enabled
                and self.archive_after_days >= self.hot_retention_days
            ):
                raise ValueError(
                    f"archive_after_days ({self.archive_after_days}) must be "
                    f"less than hot_retention_days ({self.hot_retention_days})"
                )
            if (
                self.archive_enabled
                and self.compress_after_days > self.archive_after_days
            ):
                raise ValueError(
                    f"compress_after_days ({self.compress_after_days}) should be "
                    f"<= archive_after_days ({self.archive_after_days}) — "
                    f"compress before archiving for smaller Parquet files"
                )
        assert self.archive_compression in {"zstd", "snappy", "gzip", "none"}, (
            f"Unknown compression: {self.archive_compression}"
        )


# ─────────────────────────────────────────────
# Privacy configuration
# ─────────────────────────────────────────────


@dataclass(frozen=True)
class PrivacyConfig:
    """
    Current privacy settings for this node.

    Applied by the query engine at serve time via privacy.serve_record().
    Never applied at ingest time — raw full-precision data is always stored.

    This means privacy settings can change at any time and take effect
    immediately on all future queries with no reprocessing needed.
    """

    # Dimension 1 — Location privacy
    bbox_mode: str = "snapped"  # snapped|fuzzy|region|exact|none
    bbox_snap_degrees: float = 1.0  # 0.1|0.25|0.5|1.0|2.0
    bbox_fuzz_km: float = 0.0
    exclusion_zone_km: int = 25  # 0 = disabled
    exclusion_lat: float | None = None  # Center of exclusion zone
    exclusion_lon: float | None = None
    disclose_exclusion_zone: bool = False

    # Dimension 2 — Data precision
    coordinate_dp: int = 2  # Decimal places: 1=~11km 2=~1.1km 3=~110m
    altitude_precision: str = "100ft"  # 10000ft|1000ft|100ft|exact|stripped
    rssi_mode: str = "strip"  # strip|bucketed|quantized|full
    vector_precision: str = "low"  # low|medium|full
    include_raw_messages: bool = False

    # Dimension 3 — Access control
    access_mode: str = "public"  # public|key_required|allowlist|private
    discoverable: bool = True

    # Dimension 4 — Temporal control
    embargo_minutes: int = 0
    retention_days: int = 365
    honor_ladd_requests: bool = True
    persist_on_exit: bool = False

    def __post_init__(self) -> None:
        assert 1 <= self.coordinate_dp <= 5, (
            f"coordinate_dp must be 1–5, got {self.coordinate_dp}"
        )
        assert self.altitude_precision in ALTITUDE_PRECISIONS, (
            f"Unknown altitude_precision: {self.altitude_precision}"
        )
        assert self.rssi_mode in RSSI_MODES, f"Unknown rssi_mode: {self.rssi_mode}"
        assert self.embargo_minutes >= 0
        assert self.exclusion_zone_km >= 0
        assert self.access_mode in ACCESS_MODES, (
            f"Unknown access_mode: {self.access_mode}"
        )


# ─────────────────────────────────────────────
# Query policy configuration
# ─────────────────────────────────────────────


@dataclass(frozen=True)
class QueryPolicyConfig:
    """
    Operator-configurable query policy settings.

    Note: require_spatial_filter and require_time_filter are always True
    at the protocol level and cannot be disabled. They exist here only
    for documentation purposes.
    """

    # Rate limiting
    rate_limit_enabled: bool = True
    requests_per_minute: int = 60
    burst_allowance: int = 10
    max_concurrent_queries: int = 5

    # Spatial and time filters — ALWAYS required (not operator-configurable)
    require_spatial_filter: bool = True
    require_time_filter: bool = True

    # Extent limits
    max_bbox_degrees: float = 5.0
    max_radius_km: float = 500.0
    max_time_range_hours: float = 24.0
    max_records_per_query: int = 10_000

    # Payload type restrictions
    allowed_payload_types: frozenset[str] = frozenset(PAYLOAD_TYPES)

    # Consumer access
    access_mode: str = "public"
    blocked_keys: tuple[str, ...] = ()
    blocked_ips: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        # Enforce protocol-level requirements
        object.__setattr__(self, "require_spatial_filter", True)
        object.__setattr__(self, "require_time_filter", True)

        assert self.max_bbox_degrees > 0
        assert self.max_radius_km > 0
        assert self.max_time_range_hours > 0
        assert self.max_records_per_query > 0
        assert self.access_mode in ACCESS_MODES


# ─────────────────────────────────────────────
# Parquet schemas
# ─────────────────────────────────────────────

# Core schema — all observation types
# Matches the stored record schema v0.4
coral_core_schema = pa.schema(
    [
        # Layer 1: Spatiotemporal core (raw, full precision)
        pa.field("timestamp_us", pa.int64(), nullable=False),
        pa.field("lat", pa.float64(), nullable=False),
        pa.field("lon", pa.float64(), nullable=False),
        pa.field("altitude_m", pa.float32(), nullable=True),
        pa.field("altitude_type", pa.string(), nullable=True),
        pa.field("source_id", pa.string(), nullable=True),
        # Layer 2: Provenance
        pa.field("record_id", pa.string(), nullable=False),
        pa.field("node_id", pa.string(), nullable=False),
        pa.field("payload_type", pa.string(), nullable=False),
        pa.field("payload_schema", pa.string(), nullable=False),
        pa.field("payload_schema_version", pa.string(), nullable=False),
        pa.field("ingested_at_us", pa.int64(), nullable=False),
        pa.field("signed_at_us", pa.int64(), nullable=False),
        pa.field("signature", pa.string(), nullable=False),
        # Layer 4: Tags (stored as JSON string in Parquet)
        pa.field("tags", pa.string(), nullable=True),
    ]
)

# ADS-B schema — core + flattened payload fields
# Payload fields are flattened for columnar compression efficiency
acropora_adsb_v1_schema = pa.schema(
    [
        *coral_core_schema,
        # Flattened acropora/adsb/v1 payload
        pa.field("callsign", pa.string(), nullable=True),
        pa.field("squawk", pa.string(), nullable=True),
        pa.field("altitude_baro_ft", pa.int32(), nullable=True),
        pa.field("ground_speed_kt", pa.float32(), nullable=True),
        pa.field("track_deg", pa.float32(), nullable=True),
        pa.field("vertical_rate_fpm", pa.int32(), nullable=True),
        pa.field("nic", pa.int8(), nullable=True),
        pa.field("nac_p", pa.int8(), nullable=True),
        pa.field("category", pa.string(), nullable=True),
        pa.field("rssi_dbm", pa.float32(), nullable=True),
        pa.field("messages_seen", pa.int32(), nullable=True),
    ]
)

# Registry of Parquet schemas by payload_type
# Unknown types fall back to core schema with JSONB payload as string
PARQUET_SCHEMAS: dict[str, pa.Schema] = {
    "adsb": acropora_adsb_v1_schema,
    "ais": coral_core_schema,  # Future: dedicated AIS schema
    "aprs": coral_core_schema,  # Future: dedicated APRS schema
    "weather": coral_core_schema,  # Future: dedicated weather schema
    "seismic": coral_core_schema,
    "custom": coral_core_schema,
}
