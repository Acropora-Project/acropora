"""
Tests for acropora_store/schema.py

Validates configuration dataclass construction and validation rules.
No database required.
"""

import pytest

from acropora_store.schema import (
    PrivacyConfig,
    QueryPolicyConfig,
    StorageConfig,
)


class TestStorageConfig:

    def test_default_construction(self):
        config = StorageConfig()
        assert config.hot_retention_days == 90
        assert config.compress_after_days == 7
        assert config.archive_enabled is True
        assert config.archive_after_days == 85

    def test_valid_custom_config(self):
        config = StorageConfig(
            hot_retention_days=180,
            compress_after_days=14,
            archive_after_days=170,
        )
        assert config.hot_retention_days == 180

    def test_compress_after_must_be_less_than_retention(self):
        with pytest.raises(ValueError, match="compress_after_days"):
            StorageConfig(
                hot_retention_days=30,
                compress_after_days=30,   # equal — not allowed
            )

    def test_archive_after_must_be_less_than_retention(self):
        with pytest.raises(ValueError, match="archive_after_days"):
            StorageConfig(
                hot_retention_days=30,
                compress_after_days=7,
                archive_after_days=30,    # equal — not allowed
            )

    def test_invalid_compression_format(self):
        with pytest.raises(AssertionError):
            StorageConfig(archive_compression="brotli")

    def test_valid_compression_formats(self):
        for fmt in ("zstd", "snappy", "gzip", "none"):
            config = StorageConfig(archive_compression=fmt)
            assert config.archive_compression == fmt

    def test_aggregates_disabled_by_default(self):
        config = StorageConfig()
        assert config.aggregates_enabled is False
        assert config.expose_density_api is False


class TestPrivacyConfig:

    def test_default_construction(self):
        config = PrivacyConfig()
        assert config.coordinate_dp == 2
        assert config.rssi_mode == "strip"
        assert config.access_mode == "public"
        assert config.exclusion_zone_km == 25

    def test_coordinate_dp_bounds(self):
        PrivacyConfig(coordinate_dp=1)   # valid
        PrivacyConfig(coordinate_dp=5)   # valid
        with pytest.raises(AssertionError):
            PrivacyConfig(coordinate_dp=0)
        with pytest.raises(AssertionError):
            PrivacyConfig(coordinate_dp=6)

    def test_invalid_rssi_mode(self):
        with pytest.raises(AssertionError):
            PrivacyConfig(rssi_mode="partial")

    def test_invalid_altitude_precision(self):
        with pytest.raises(AssertionError):
            PrivacyConfig(altitude_precision="50ft")

    def test_invalid_access_mode(self):
        with pytest.raises(AssertionError):
            PrivacyConfig(access_mode="friends_only")

    def test_negative_embargo_not_allowed(self):
        with pytest.raises(AssertionError):
            PrivacyConfig(embargo_minutes=-1)

    def test_negative_exclusion_zone_not_allowed(self):
        with pytest.raises(AssertionError):
            PrivacyConfig(exclusion_zone_km=-1)

    def test_all_valid_rssi_modes(self):
        for mode in ("strip", "bucketed", "quantized", "full"):
            config = PrivacyConfig(rssi_mode=mode)
            assert config.rssi_mode == mode

    def test_all_valid_altitude_precisions(self):
        for prec in ("10000ft", "1000ft", "100ft", "exact", "stripped"):
            config = PrivacyConfig(altitude_precision=prec)
            assert config.altitude_precision == prec


class TestQueryPolicyConfig:

    def test_default_construction(self):
        config = QueryPolicyConfig()
        assert config.max_time_range_hours == 24.0
        assert config.max_records_per_query == 10_000
        assert config.rate_limit_enabled is True

    def test_spatial_filter_always_required(self):
        """require_spatial_filter must always be True — not operator-configurable."""
        config = QueryPolicyConfig(require_spatial_filter=False)
        assert config.require_spatial_filter is True

    def test_time_filter_always_required(self):
        """require_time_filter must always be True — not operator-configurable."""
        config = QueryPolicyConfig(require_time_filter=False)
        assert config.require_time_filter is True

    def test_invalid_max_bbox_degrees(self):
        with pytest.raises(AssertionError):
            QueryPolicyConfig(max_bbox_degrees=0)

    def test_invalid_max_radius_km(self):
        with pytest.raises(AssertionError):
            QueryPolicyConfig(max_radius_km=-10)