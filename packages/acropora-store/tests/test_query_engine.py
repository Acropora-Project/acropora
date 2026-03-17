"""
Tests for acropora_store/query_engine.py

Validates spatial filters, time range, and query model construction.
No database required.
"""

import time

import pytest

from acropora_store.query_engine import (
    BoundingBox,
    LocationRadius,
    ObservationQuery,
    TimeRange,
)


class TestBoundingBox:

    def test_valid_construction(self):
        bbox = BoundingBox(32.5, -98.5, 33.5, -96.5)
        assert bbox.lat_min == 32.5
        assert bbox.lon_max == -96.5

    def test_lat_min_must_be_less_than_lat_max(self):
        with pytest.raises(ValueError):
            BoundingBox(33.5, -98.5, 32.5, -96.5)

    def test_lat_min_equal_lat_max_rejected(self):
        with pytest.raises(ValueError):
            BoundingBox(32.5, -98.5, 32.5, -96.5)

    def test_lon_min_must_be_less_than_lon_max(self):
        with pytest.raises(ValueError):
            BoundingBox(32.5, -96.5, 33.5, -98.5)

    def test_lat_out_of_range(self):
        with pytest.raises(ValueError):
            BoundingBox(-95.0, -98.5, 33.5, -96.5)

    def test_lon_out_of_range(self):
        with pytest.raises(ValueError):
            BoundingBox(32.5, -185.0, 33.5, -96.5)

    def test_degrees_span(self):
        bbox = BoundingBox(32.5, -98.5, 33.5, -96.5)
        # lat span = 1.0, lon span = 2.0 → max is 2.0
        assert bbox.degrees_span == pytest.approx(2.0)

    def test_center(self):
        bbox = BoundingBox(32.0, -98.0, 34.0, -96.0)
        lat, lon = bbox.center
        assert lat == pytest.approx(33.0)
        assert lon == pytest.approx(-97.0)

    def test_from_string_valid(self):
        bbox = BoundingBox.from_string("32.5,-98.5,33.5,-96.5")
        assert bbox.lat_min == pytest.approx(32.5)
        assert bbox.lon_max == pytest.approx(-96.5)

    def test_from_string_invalid_parts(self):
        with pytest.raises(ValueError):
            BoundingBox.from_string("32.5,-98.5,33.5")   # only 3 values

    def test_from_string_non_numeric(self):
        with pytest.raises(ValueError):
            BoundingBox.from_string("32.5,north,33.5,-96.5")


class TestLocationRadius:

    def test_valid_construction(self):
        loc = LocationRadius(32.9, -97.0, 100.0)
        assert loc.lat == 32.9
        assert loc.radius_km == 100.0

    def test_radius_must_be_positive(self):
        with pytest.raises(ValueError):
            LocationRadius(32.9, -97.0, 0)
        with pytest.raises(ValueError):
            LocationRadius(32.9, -97.0, -50)

    def test_invalid_lat(self):
        with pytest.raises(ValueError):
            LocationRadius(95.0, -97.0, 100.0)

    def test_invalid_lon(self):
        with pytest.raises(ValueError):
            LocationRadius(32.9, 200.0, 100.0)

    def test_radius_m_conversion(self):
        loc = LocationRadius(32.9, -97.0, 100.0)
        assert loc.radius_m == pytest.approx(100_000.0)

    def test_to_bbox_contains_center(self):
        loc = LocationRadius(32.9, -97.0, 100.0)
        bbox = loc.to_bbox()
        assert bbox.lat_min < 32.9 < bbox.lat_max
        assert bbox.lon_min < -97.0 < bbox.lon_max

    def test_to_bbox_is_larger_than_radius(self):
        """The bounding box should over-estimate, never under-estimate."""
        loc = LocationRadius(32.9, -97.0, 100.0)
        bbox = loc.to_bbox()
        # At ~111km per degree of latitude, 100km should give ~0.9 degrees
        lat_half_span = (bbox.lat_max - bbox.lat_min) / 2
        assert lat_half_span >= 0.9

    def test_from_strings_valid(self):
        loc = LocationRadius.from_strings("32.9,-97.0", 100.0)
        assert loc.lat == pytest.approx(32.9)
        assert loc.lon == pytest.approx(-97.0)
        assert loc.radius_km == 100.0

    def test_from_strings_invalid_location(self):
        with pytest.raises(ValueError):
            LocationRadius.from_strings("32.9", 100.0)   # missing lon


class TestTimeRange:

    def test_valid_construction(self):
        tr = TimeRange(start_us=1000, end_us=2000)
        assert tr.start_us == 1000
        assert tr.end_us == 2000

    def test_start_must_be_before_end(self):
        with pytest.raises(ValueError):
            TimeRange(start_us=2000, end_us=1000)

    def test_equal_start_end_rejected(self):
        with pytest.raises(ValueError):
            TimeRange(start_us=1000, end_us=1000)

    def test_duration_hours(self):
        one_hour_us = 3_600_000_000
        tr = TimeRange(start_us=0, end_us=one_hour_us)
        assert tr.duration_hours == pytest.approx(1.0)

    def test_last_hours_constructor(self):
        tr = TimeRange.last_hours(24)
        now_us = time.time_ns() // 1000
        assert tr.end_us == pytest.approx(now_us, abs=1_000_000)   # within 1s
        assert tr.duration_hours == pytest.approx(24.0, abs=0.01)

    def test_last_hours_zero_rejected(self):
        with pytest.raises(ValueError):
            TimeRange.last_hours(0)

    def test_last_hours_negative_rejected(self):
        with pytest.raises(ValueError):
            TimeRange.last_hours(-1)


class TestObservationQuery:

    def test_valid_construction(self):
        query = ObservationQuery(
            time_range=TimeRange.last_hours(24),
            spatial_filter=BoundingBox(32.5, -98.5, 33.5, -96.5),
        )
        assert query.limit == 1000
        assert query.order == "desc"
        assert query.payload_type is None