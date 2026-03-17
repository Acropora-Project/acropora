"""
Shared fixtures for acropora-store tests.
All fixtures here are pure Python — no database required.
"""

import pytest
from acropora_store.schema import PrivacyConfig, QueryPolicyConfig, StorageConfig


@pytest.fixture
def default_privacy() -> PrivacyConfig:
    return PrivacyConfig()


@pytest.fixture
def default_storage() -> StorageConfig:
    return StorageConfig()


@pytest.fixture
def default_query_policy() -> QueryPolicyConfig:
    return QueryPolicyConfig()


@pytest.fixture
def privacy_with_exclusion() -> PrivacyConfig:
    """Privacy config with a 25km exclusion zone centered on DFW."""
    return PrivacyConfig(
        exclusion_zone_km=25,
        exclusion_lat=32.9,
        exclusion_lon=-97.0,
    )


@pytest.fixture
def privacy_with_embargo() -> PrivacyConfig:
    """Privacy config with a 30-minute embargo."""
    return PrivacyConfig(embargo_minutes=30)


@pytest.fixture
def raw_adsb_record() -> dict:
    """A minimal valid raw stored ADS-B record."""
    return {
        "timestamp_us": 1741820400000000,
        "lat": 32.896894,
        "lon": -97.037996,
        "altitude_m": 10058.4,
        "altitude_type": "barometric",
        "source_id": "a1b2c3",
        "record_id": "a" * 64,
        "node_id": "b" * 64,
        "payload_type": "adsb",
        "payload_schema": "acropora/adsb/v1",
        "payload_schema_version": "1.0.0",
        "ingested_at_us": 1741820400100000,
        "signed_at_us": 1741820400200000,
        "signature": "dGVzdA==",
        "payload": {
            "callsign": "AAL123  ",
            "squawk": "1234",
            "altitude_baro_ft": 33000,
            "ground_speed_kt": 450.0,
            "track_deg": 90.0,
            "vertical_rate_fpm": 0,
            "nic": 8,
            "nac_p": 9,
            "category": "A3",
            "rssi_dbm": -45.2,
            "messages_seen": 12,
        },
        "tags": None,
    }
