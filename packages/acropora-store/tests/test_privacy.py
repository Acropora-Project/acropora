"""
Tests for acropora_store/privacy.py

Validates privacy transformations applied at serve time.
No database required.
"""

import time

import pytest
from acropora_store.privacy import (
    quantize_altitude,
    quantize_coordinate,
    serve_record,
    settings_version,
    within_exclusion_zone,
)
from acropora_store.schema import PrivacyConfig


class TestWithinExclusionZone:
    def test_disabled_when_zero_km(self):
        config = PrivacyConfig(exclusion_zone_km=0)
        assert within_exclusion_zone(32.9, -97.0, config) is False

    def test_disabled_when_no_center(self):
        config = PrivacyConfig(
            exclusion_zone_km=25,
            exclusion_lat=None,
            exclusion_lon=None,
        )
        assert within_exclusion_zone(32.9, -97.0, config) is False

    def test_point_inside_zone(self, privacy_with_exclusion):
        # Center itself should be within the zone
        assert within_exclusion_zone(32.9, -97.0, privacy_with_exclusion) is True

    def test_point_just_inside_zone(self, privacy_with_exclusion):
        # ~10km north of center — within 25km zone
        assert within_exclusion_zone(33.0, -97.0, privacy_with_exclusion) is True

    def test_point_outside_zone(self, privacy_with_exclusion):
        # ~111km north of center — outside 25km zone
        assert within_exclusion_zone(33.9, -97.0, privacy_with_exclusion) is False


class TestQuantizeCoordinate:
    def test_quantize_to_2dp(self):
        assert quantize_coordinate(32.896894, 2) == 32.9
        assert quantize_coordinate(-97.037996, 2) == -97.04

    def test_quantize_to_5dp(self):
        result = quantize_coordinate(32.896894, 5)
        assert result == 32.89689

    def test_quantize_to_1dp(self):
        assert quantize_coordinate(32.896894, 1) == 32.9


class TestQuantizeAltitude:
    def test_none_passthrough(self):
        assert quantize_altitude(None, "100ft") is None

    def test_stripped(self):
        assert quantize_altitude(10000.0, "stripped") is None

    def test_exact(self):
        assert quantize_altitude(10058.4, "exact") == 10058.4

    def test_100ft_precision(self):
        # 10058.4m → nearest 30.48m step
        result = quantize_altitude(10058.4, "100ft")
        assert result is not None
        assert result % 30.48 == pytest.approx(10058.4 % 30.48, abs=0.01)

    def test_1000ft_precision(self):
        result = quantize_altitude(10058.4, "1000ft")
        assert result is not None
        assert result % 304.8 == pytest.approx(10058.4 % 304.8, abs=0.01)


class TestServeRecord:
    def test_serves_valid_record(self, raw_adsb_record, default_privacy):
        record = serve_record(raw_adsb_record, default_privacy, "node_abc")
        assert record is not None
        assert record.source_id == "a1b2c3"
        assert record.payload_type == "adsb"

    def test_coordinates_quantized(self, raw_adsb_record, default_privacy):
        record = serve_record(raw_adsb_record, default_privacy, "node_abc")
        assert record is not None
        # default coordinate_dp=2
        assert record.lat == round(raw_adsb_record["lat"], 2)
        assert record.lon == round(raw_adsb_record["lon"], 2)

    def test_rssi_stripped_by_default(self, raw_adsb_record, default_privacy):
        # default rssi_mode=strip
        record = serve_record(raw_adsb_record, default_privacy, "node_abc")
        assert record is not None
        assert record.payload["rssi_dbm"] is None

    def test_rssi_full_when_configured(self, raw_adsb_record):
        config = PrivacyConfig(rssi_mode="full")
        record = serve_record(raw_adsb_record, config, "node_abc")
        assert record is not None
        assert record.payload["rssi_dbm"] == pytest.approx(-45.2)

    def test_rssi_bucketed(self, raw_adsb_record):
        config = PrivacyConfig(rssi_mode="bucketed")
        record = serve_record(raw_adsb_record, config, "node_abc")
        assert record is not None
        # -45.2 dBm falls in "medium" bucket (-60 to -30)
        assert record.payload["rssi_dbm"] == "medium"

    def test_rssi_quantized(self, raw_adsb_record):
        config = PrivacyConfig(rssi_mode="quantized")
        record = serve_record(raw_adsb_record, config, "node_abc")
        assert record is not None
        # -45.2 → nearest 5 dBm = -45
        assert record.payload["rssi_dbm"] == pytest.approx(-45.0)

    def test_exclusion_zone_returns_none(self, raw_adsb_record, privacy_with_exclusion):
        # raw_adsb_record is at 32.9, -97.0 — the center of the exclusion zone
        result = serve_record(raw_adsb_record, privacy_with_exclusion, "node_abc")
        assert result is None

    def test_outside_exclusion_zone_serves(self, raw_adsb_record):
        config = PrivacyConfig(
            exclusion_zone_km=25,
            exclusion_lat=40.0,  # New York area — far from DFW record
            exclusion_lon=-74.0,
        )
        result = serve_record(raw_adsb_record, config, "node_abc")
        assert result is not None

    def test_embargo_withholds_recent_record(self, raw_adsb_record):
        config = PrivacyConfig(embargo_minutes=60)
        # Use a timestamp 1 minute ago — within the 60 minute embargo
        recent_record = dict(raw_adsb_record)
        recent_record["timestamp_us"] = time.time_ns() // 1000 - 60_000_000
        result = serve_record(recent_record, config, "node_abc")
        assert result is None

    def test_embargo_serves_old_record(self, raw_adsb_record, privacy_with_embargo):
        # raw_adsb_record timestamp is from 2025 — well outside any embargo
        result = serve_record(raw_adsb_record, privacy_with_embargo, "node_abc")
        assert result is not None

    def test_privacy_metadata_attached(self, raw_adsb_record, default_privacy):
        record = serve_record(raw_adsb_record, default_privacy, "node_abc")
        assert record is not None
        assert record.privacy.coordinate_dp == 2
        assert record.privacy.rssi_stripped is True
        assert record.privacy.settings_version != ""

    def test_ingested_at_not_in_served_record(self, raw_adsb_record, default_privacy):
        """ingested_at_us is an internal field — must not appear in served records."""
        record = serve_record(raw_adsb_record, default_privacy, "node_abc")
        assert record is not None
        assert not hasattr(record, "ingested_at_us")


class TestSettingsVersion:
    def test_same_config_same_hash(self):
        config = PrivacyConfig()
        assert settings_version(config) == settings_version(config)

    def test_different_config_different_hash(self):
        config_a = PrivacyConfig(coordinate_dp=2)
        config_b = PrivacyConfig(coordinate_dp=3)
        assert settings_version(config_a) != settings_version(config_b)

    def test_hash_is_16_chars(self):
        assert len(settings_version(PrivacyConfig())) == 16
