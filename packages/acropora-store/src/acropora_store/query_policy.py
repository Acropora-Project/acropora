"""
acropora_store/query_policy.py

Query policy enforcement.

Validates incoming queries against the node's configured query policy
before any SQL is executed. Raises QueryPolicyViolationError with a clear
field-level error message if any limit is exceeded.
"""

from __future__ import annotations

from acropora_store.query_engine import (
    BoundingBox,
    LocationRadius,
    ObservationQuery,
    SourceQuery,
    SpatialFilter,
)
from acropora_store.schema import QueryPolicyConfig


class QueryPolicyViolationError(Exception):
    """
    Raised when a query exceeds a configured policy limit.
    Carries the field name that caused the violation for structured
    error responses.
    """

    def __init__(self, message: str, field: str) -> None:
        super().__init__(message)
        self.field = field


def _enforce_spatial_extent(
    spatial_filter: SpatialFilter,
    policy: QueryPolicyConfig,
) -> None:
    """Enforce spatial extent limits based on filter type."""
    if isinstance(spatial_filter, BoundingBox):
        if spatial_filter.degrees_span > policy.max_bbox_degrees:
            raise QueryPolicyViolationError(
                f"Bounding box span {spatial_filter.degrees_span:.2f}° exceeds "
                f"the maximum {policy.max_bbox_degrees}° for this node. "
                f"Reduce the query area or use a location+radius query.",
                field="bbox",
            )

    elif isinstance(spatial_filter, LocationRadius):
        if spatial_filter.radius_km > policy.max_radius_km:
            raise QueryPolicyViolationError(
                f"Radius {spatial_filter.radius_km}km exceeds "
                f"the maximum {policy.max_radius_km}km for this node. "
                f"Reduce the radius or use a bounding box query.",
                field="radius_km",
            )


def enforce_observation_query_policy(
    query: ObservationQuery,
    policy: QueryPolicyConfig,
) -> None:
    """
    Validate an observation query against the node's query policy.
    Raises QueryPolicyViolationError if any limit is exceeded.
    Called before any SQL execution.
    """
    # Time range duration
    if query.time_range.duration_hours > policy.max_time_range_hours:
        raise QueryPolicyViolationError(
            f"Time range {query.time_range.duration_hours:.1f}h exceeds "
            f"the maximum {policy.max_time_range_hours}h for this node.",
            field="time_range",
        )

    # Spatial extent
    _enforce_spatial_extent(query.spatial_filter, policy)

    # Payload type allowed
    if (
        query.payload_type is not None
        and query.payload_type not in policy.allowed_payload_types
    ):
        raise QueryPolicyViolationError(
            f"Payload type '{query.payload_type}' is not served by this node. "
            f"Available types: {sorted(policy.allowed_payload_types)}",
            field="payload_type",
        )

    # Limit cap
    if query.limit > policy.max_records_per_query:
        raise QueryPolicyViolationError(
            f"Requested limit {query.limit} exceeds "
            f"the maximum {policy.max_records_per_query} for this node.",
            field="limit",
        )


def enforce_source_query_policy(
    query: SourceQuery,
    policy: QueryPolicyConfig,
) -> None:
    """
    Validate a source query against the node's query policy.
    Source queries have the same spatial and time requirements.
    """
    if query.time_range.duration_hours > policy.max_time_range_hours:
        raise QueryPolicyViolationError(
            f"Time range {query.time_range.duration_hours:.1f}h exceeds "
            f"the maximum {policy.max_time_range_hours}h for this node.",
            field="time_range",
        )

    _enforce_spatial_extent(query.spatial_filter, policy)
