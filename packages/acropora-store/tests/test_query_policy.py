"""
Tests for acropora_store/query_policy.py

Validates query policy enforcement — all pure Python, no database.
"""

import pytest
from acropora_store.query_engine import (
    BoundingBox,
    LocationRadius,
    ObservationQuery,
    SourceQuery,
    TimeRange,
)
from acropora_store.query_policy import (
    QueryPolicyViolationError,
    enforce_observation_query_policy,
    enforce_source_query_policy,
)
from acropora_store.schema import QueryPolicyConfig


@pytest.fixture
def strict_policy() -> QueryPolicyConfig:
    """A tight policy for testing limit enforcement."""
    return QueryPolicyConfig(
        max_time_range_hours=24.0,
        max_bbox_degrees=5.0,
        max_radius_km=500.0,
        max_records_per_query=1000,
    )


@pytest.fixture
def bbox_query() -> ObservationQuery:
    return ObservationQuery(
        time_range=TimeRange.last_hours(1),
        spatial_filter=BoundingBox(32.5, -98.5, 33.5, -96.5),
    )


@pytest.fixture
def radius_query() -> ObservationQuery:
    return ObservationQuery(
        time_range=TimeRange.last_hours(1),
        spatial_filter=LocationRadius(32.9, -97.0, 100.0),
    )


class TestObservationQueryPolicy:
    def test_valid_query_passes(self, bbox_query, strict_policy):
        # Should not raise
        enforce_observation_query_policy(bbox_query, strict_policy)

    def test_time_range_exceeded(self, strict_policy):
        query = ObservationQuery(
            time_range=TimeRange.last_hours(48),  # exceeds 24h limit
            spatial_filter=BoundingBox(32.5, -98.5, 33.5, -96.5),
        )
        with pytest.raises(QueryPolicyViolationError) as exc_info:
            enforce_observation_query_policy(query, strict_policy)
        assert exc_info.value.field == "time_range"

    def test_bbox_degrees_exceeded(self, strict_policy):
        query = ObservationQuery(
            time_range=TimeRange.last_hours(1),
            spatial_filter=BoundingBox(30.0, -100.0, 36.0, -94.0),  # 6° span
        )
        with pytest.raises(QueryPolicyViolationError) as exc_info:
            enforce_observation_query_policy(query, strict_policy)
        assert exc_info.value.field == "bbox"

    def test_radius_exceeded(self, strict_policy):
        query = ObservationQuery(
            time_range=TimeRange.last_hours(1),
            spatial_filter=LocationRadius(32.9, -97.0, 1000.0),  # 1000km > 500km
        )
        with pytest.raises(QueryPolicyViolationError) as exc_info:
            enforce_observation_query_policy(query, strict_policy)
        assert exc_info.value.field == "radius_km"

    def test_payload_type_not_allowed(self, strict_policy):

        policy = QueryPolicyConfig(
            allowed_payload_types=frozenset({"adsb"}),
        )
        query = ObservationQuery(
            time_range=TimeRange.last_hours(1),
            spatial_filter=BoundingBox(32.5, -98.5, 33.5, -96.5),
            payload_type="ais",  # not in allowed set
        )
        with pytest.raises(QueryPolicyViolationError) as exc_info:
            enforce_observation_query_policy(query, policy)
        assert exc_info.value.field == "payload_type"

    def test_allowed_payload_type_passes(self, strict_policy):
        query = ObservationQuery(
            time_range=TimeRange.last_hours(1),
            spatial_filter=BoundingBox(32.5, -98.5, 33.5, -96.5),
            payload_type="adsb",
        )
        enforce_observation_query_policy(query, strict_policy)  # should not raise

    def test_limit_exceeded(self, strict_policy):
        query = ObservationQuery(
            time_range=TimeRange.last_hours(1),
            spatial_filter=BoundingBox(32.5, -98.5, 33.5, -96.5),
            limit=5000,  # exceeds max_records_per_query=1000
        )
        with pytest.raises(QueryPolicyViolationError) as exc_info:
            enforce_observation_query_policy(query, strict_policy)
        assert exc_info.value.field == "limit"

    def test_violation_has_message(self, strict_policy):
        query = ObservationQuery(
            time_range=TimeRange.last_hours(48),
            spatial_filter=BoundingBox(32.5, -98.5, 33.5, -96.5),
        )
        with pytest.raises(QueryPolicyViolationError) as exc_info:
            enforce_observation_query_policy(query, strict_policy)
        assert str(exc_info.value) != ""


class TestSourceQueryPolicy:
    def test_valid_source_query_passes(self, strict_policy):
        query = SourceQuery(
            source_id="a1b2c3",
            time_range=TimeRange.last_hours(1),
            spatial_filter=BoundingBox(32.5, -98.5, 33.5, -96.5),
        )
        enforce_source_query_policy(query, strict_policy)  # should not raise

    def test_time_range_exceeded(self, strict_policy):
        query = SourceQuery(
            source_id="a1b2c3",
            time_range=TimeRange.last_hours(48),
            spatial_filter=BoundingBox(32.5, -98.5, 33.5, -96.5),
        )
        with pytest.raises(QueryPolicyViolationError) as exc_info:
            enforce_source_query_policy(query, strict_policy)
        assert exc_info.value.field == "time_range"

    def test_radius_exceeded(self, strict_policy):
        query = SourceQuery(
            source_id="a1b2c3",
            time_range=TimeRange.last_hours(1),
            spatial_filter=LocationRadius(32.9, -97.0, 1000.0),
        )
        with pytest.raises(QueryPolicyViolationError) as exc_info:
            enforce_source_query_policy(query, strict_policy)
        assert exc_info.value.field == "radius_km"
