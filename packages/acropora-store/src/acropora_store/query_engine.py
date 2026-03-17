"""
acropora_store/query_engine.py

Query request and response models for the Reef Protocol query API.

These dataclasses are the internal representation of queries and
results — separate from the Pydantic models used by FastAPI routes.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass

# ─────────────────────────────────────────────
# Spatial filter types
# ─────────────────────────────────────────────


@dataclass(frozen=True)
class BoundingBox:
    """
    Rectangular spatial filter defined by two lat/lon corners.
    Both a spatial filter for queries and a coverage descriptor
    in node advertisements.
    """

    lat_min: float
    lon_min: float
    lat_max: float
    lon_max: float

    def __post_init__(self) -> None:
        if not (-90 <= self.lat_min < self.lat_max <= 90):
            raise ValueError(
                f"Invalid latitude range: [{self.lat_min}, {self.lat_max}]. "
                f"lat_min must be less than lat_max and both in [-90, 90]."
            )
        if not (-180 <= self.lon_min < self.lon_max <= 180):
            raise ValueError(
                f"Invalid longitude range: [{self.lon_min}, {self.lon_max}]. "
                f"lon_min must be less than lon_max and both in [-180, 180]."
            )

    @property
    def degrees_span(self) -> float:
        """Largest dimension in degrees — used for query policy enforcement."""
        return max(
            self.lat_max - self.lat_min,
            self.lon_max - self.lon_min,
        )

    @property
    def center(self) -> tuple[float, float]:
        """(lat, lon) center of the bounding box."""
        return (
            (self.lat_min + self.lat_max) / 2,
            (self.lon_min + self.lon_max) / 2,
        )

    @classmethod
    def from_string(cls, s: str) -> BoundingBox:
        """
        Parse from query parameter string: "lat_min,lon_min,lat_max,lon_max"
        e.g. "32.5,-98.5,33.5,-96.5"
        """
        try:
            parts = [float(x.strip()) for x in s.split(",")]
            if len(parts) != 4:
                raise ValueError("bbox must have exactly 4 comma-separated values")
            return cls(
                lat_min=parts[0],
                lon_min=parts[1],
                lat_max=parts[2],
                lon_max=parts[3],
            )
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Invalid bbox format: {e}. "
                f"Expected lat_min,lon_min,lat_max,lon_max — "
                f"e.g. '32.5,-98.5,33.5,-96.5'"
            ) from e


@dataclass(frozen=True)
class LocationRadius:
    """
    Circular spatial filter defined by a center point and radius.
    Uses great-circle distance — accurate for any size radius.
    """

    lat: float
    lon: float
    radius_km: float

    def __post_init__(self) -> None:
        if not (-90 <= self.lat <= 90):
            raise ValueError(f"Invalid latitude: {self.lat}")
        if not (-180 <= self.lon <= 180):
            raise ValueError(f"Invalid longitude: {self.lon}")
        if self.radius_km <= 0:
            raise ValueError(f"radius_km must be positive, got {self.radius_km}")

    @property
    def radius_m(self) -> float:
        """Radius in meters — used with PostGIS ST_DWithin geography queries."""
        return self.radius_km * 1000.0

    def to_bbox(self) -> BoundingBox:
        """
        Approximate bounding box enclosing this circle.
        Used as a pre-filter before the precise ST_DWithin check.
        Slightly over-estimates to ensure no points are excluded.
        """
        lat_delta = math.degrees(self.radius_km / 6371.0)
        cos_lat = math.cos(math.radians(self.lat))
        # Avoid division by zero near poles
        lon_delta = math.degrees(self.radius_km / (6371.0 * max(cos_lat, 0.001)))
        return BoundingBox(
            lat_min=max(-90.0, self.lat - lat_delta),
            lon_min=max(-180.0, self.lon - lon_delta),
            lat_max=min(90.0, self.lat + lat_delta),
            lon_max=min(180.0, self.lon + lon_delta),
        )

    @classmethod
    def from_strings(cls, location: str, radius_km: float) -> LocationRadius:
        """
        Parse from query parameter strings.
        location: "lat,lon" e.g. "32.9,-97.0"
        radius_km: float
        """
        try:
            parts = [float(x.strip()) for x in location.split(",")]
            if len(parts) != 2:
                raise ValueError("location must have exactly 2 comma-separated values")
            return cls(lat=parts[0], lon=parts[1], radius_km=radius_km)
        except (ValueError, TypeError) as e:
            raise ValueError(
                f"Invalid location format: {e}. Expected lat,lon — e.g. '32.9,-97.0'"
            ) from e


# Type alias for any spatial filter
SpatialFilter = BoundingBox | LocationRadius


# ─────────────────────────────────────────────
# Time range
# ─────────────────────────────────────────────


@dataclass(frozen=True)
class TimeRange:
    """Time range specified as Unix timestamps in microseconds (UTC)."""

    start_us: int
    end_us: int

    def __post_init__(self) -> None:
        if self.start_us >= self.end_us:
            raise ValueError(
                f"start_us ({self.start_us}) must be before end_us ({self.end_us})"
            )

    @property
    def duration_hours(self) -> float:
        """Duration in hours — used for query policy enforcement."""
        return (self.end_us - self.start_us) / 3_600_000_000.0

    @classmethod
    def last_hours(cls, hours: float) -> TimeRange:
        """Convenience constructor for a recent time window."""
        if hours <= 0:
            raise ValueError(f"hours must be positive, got {hours}")
        now = time.time_ns() // 1000
        return cls(
            start_us=int(now - hours * 3_600_000_000),
            end_us=now,
        )


# ─────────────────────────────────────────────
# Query request models
# ─────────────────────────────────────────────


@dataclass(frozen=True)
class ObservationQuery:
    """
    Primary observation query.

    Both time_range and spatial_filter are required.
    This is a protocol-level constraint — not just a policy preference.

    A query without a spatial constraint would scan all locations for
    the given time period, which is expensive for the node and for
    the network (the client would need to query every node that has
    data for that time period).
    """

    time_range: TimeRange
    spatial_filter: SpatialFilter  # Required — BoundingBox or LocationRadius

    # Optional filters
    payload_type: str | None = None
    source_id: str | None = None
    altitude_min_m: float | None = None
    altitude_max_m: float | None = None

    # Pagination
    limit: int = 1000
    offset: int = 0
    order: str = "desc"  # "asc" | "desc" by timestamp_us


@dataclass(frozen=True)
class SourceQuery:
    """
    All observations for a specific source identifier within a spatial
    and temporal scope.

    Spatial filter still required — a source may have traveled through
    many regions and the client should scope the request appropriately.
    The client query planner uses the track registry to determine the
    correct spatial scope before issuing source queries.
    """

    source_id: str
    time_range: TimeRange
    spatial_filter: SpatialFilter  # Required

    limit: int = 10_000
    order: str = "asc"  # Chronological is natural for tracks


@dataclass(frozen=True)
class CoverageQuery:
    """
    What data does this node hold?

    No spatial filter required — coverage is always reported globally
    for this node's full dataset. Consumers use this to decide whether
    to query this node at all before sending an observation query.
    """

    payload_type: str | None = None  # Filter to specific type or all


# ─────────────────────────────────────────────
# Query response models
# ─────────────────────────────────────────────


@dataclass
class QueryResponse:
    """Response envelope for observation and source queries."""

    records: list  # list[ServedRecord]
    total_count: int  # Total matching before limit/offset
    returned_count: int  # Records in this response
    query_time_ms: float
    node_id: str
    served_at_us: int
    coverage_note: str | None = None  # e.g. "partial — embargo applied to N records"


@dataclass
class CoverageResponse:
    """Node coverage summary per payload type."""

    node_id: str
    payload_types: list[dict]  # Per-type coverage summary dicts
    generated_at_us: int
