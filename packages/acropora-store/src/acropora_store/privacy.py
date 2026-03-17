"""
acropora_store/privacy.py

Privacy transformation layer.
Applies operator privacy settings to observation records at query time.
Raw data is never modified — transformations happen only on served records.
"""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import dataclass

from acropora_store.schema import PrivacyConfig


def settings_version(config: PrivacyConfig) -> str:
    """Stable hash of current privacy config for change detection."""
    canonical = json.dumps(
        {
            "coordinate_dp": config.coordinate_dp,
            "altitude_precision": config.altitude_precision,
            "rssi_mode": config.rssi_mode,
            "embargo_minutes": config.embargo_minutes,
            "exclusion_zone_km": config.exclusion_zone_km,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return hashlib.sha256(canonical).hexdigest()[:16]


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres between two (lat, lon) points."""
    r = 6_371.0
    rlat1 = math.radians(lat1)
    rlat2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(rlat1) * math.cos(rlat2) * math.sin(dlon / 2) ** 2
    )
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def within_exclusion_zone(lat: float, lon: float, config: PrivacyConfig) -> bool:
    """True if coordinates fall within the node's exclusion zone."""
    if config.exclusion_zone_km <= 0:
        return False
    if config.exclusion_lat is None or config.exclusion_lon is None:
        return False
    return haversine_km(config.exclusion_lat, config.exclusion_lon, lat, lon) < config.exclusion_zone_km


def quantize_coordinate(value: float, dp: int) -> float:
    return round(value, dp)


def quantize_altitude(altitude_m: float | None, precision: str) -> float | None:
    if altitude_m is None:
        return None
    if precision == "stripped":
        return None
    if precision == "exact":
        return altitude_m
    precision_map = {"10000ft": 3048.0, "1000ft": 304.8, "100ft": 30.48}
    step_m = precision_map.get(precision, 30.48)
    return round(altitude_m / step_m) * step_m


def transform_payload(
    payload: dict,
    payload_type: str,
    config: PrivacyConfig,
) -> dict:
    """Apply privacy transformations to payload fields."""
    transformed = dict(payload)

    if payload_type == "adsb":
        if config.rssi_mode == "strip":
            transformed["rssi_dbm"] = None
        elif config.rssi_mode == "bucketed":
            rssi = transformed.get("rssi_dbm")
            if rssi is not None:
                if rssi >= -30:
                    transformed["rssi_dbm"] = "strong"
                elif rssi >= -60:
                    transformed["rssi_dbm"] = "medium"
                elif rssi >= -90:
                    transformed["rssi_dbm"] = "weak"
                else:
                    transformed["rssi_dbm"] = "very_weak"
        elif config.rssi_mode == "quantized":
            rssi = transformed.get("rssi_dbm")
            if rssi is not None:
                transformed["rssi_dbm"] = round(rssi / 5) * 5

    return transformed


@dataclass
class PrivacyMetadata:
    coordinate_dp: int
    altitude_precision: str
    rssi_stripped: bool
    embargo_minutes: int
    exclusion_zone_km: int
    settings_version: str


@dataclass
class ServedRecord:
    """An observation record with privacy transformations applied."""

    timestamp_us: int
    lat: float
    lon: float
    altitude_m: float | None
    altitude_type: str | None
    source_id: str | None
    record_id: str
    node_id: str
    payload_type: str
    payload_schema: str
    payload_schema_version: str
    signed_at_us: int
    signature: str
    payload: dict
    tags: list[list[str]] | None
    privacy: PrivacyMetadata


def serve_record(
    stored: dict,
    config: PrivacyConfig,
    node_id: str,
) -> ServedRecord | None:
    """
    Apply privacy transformations to a stored raw record.
    Returns None if the record should not be served
    (within exclusion zone or within embargo window).
    """
    if within_exclusion_zone(stored["lat"], stored["lon"], config):
        return None

    if config.embargo_minutes > 0:
        threshold_us = time.time_ns() // 1000 - config.embargo_minutes * 60 * 1_000_000
        if stored["timestamp_us"] > threshold_us:
            return None

    served_lat = quantize_coordinate(stored["lat"], config.coordinate_dp)
    served_lon = quantize_coordinate(stored["lon"], config.coordinate_dp)
    served_altitude = quantize_altitude(
        stored.get("altitude_m"), config.altitude_precision
    )
    served_payload = transform_payload(
        stored.get("payload", {}),
        stored["payload_type"],
        config,
    )

    privacy = PrivacyMetadata(
        coordinate_dp=config.coordinate_dp,
        altitude_precision=config.altitude_precision,
        rssi_stripped=config.rssi_mode == "strip",
        embargo_minutes=config.embargo_minutes,
        exclusion_zone_km=config.exclusion_zone_km,
        settings_version=settings_version(config),
    )

    return ServedRecord(
        timestamp_us=stored["timestamp_us"],
        lat=served_lat,
        lon=served_lon,
        altitude_m=served_altitude,
        altitude_type=stored.get("altitude_type"),
        source_id=stored.get("source_id"),
        record_id=stored["record_id"],
        node_id=stored["node_id"],
        payload_type=stored["payload_type"],
        payload_schema=stored["payload_schema"],
        payload_schema_version=stored["payload_schema_version"],
        signed_at_us=stored["signed_at_us"],
        signature=stored["signature"],
        payload=served_payload,
        tags=stored.get("tags"),
        privacy=privacy,
    )
