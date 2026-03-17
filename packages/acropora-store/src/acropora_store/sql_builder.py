"""
acropora_store/sql_builder.py

Parameterized SQL query builders for TimescaleDB.

Each builder returns (sql_string, params_list) suitable for
asyncpg's conn.fetch(sql, *params) calls.

All queries include the required spatial filter — this is enforced
at the query model level (ObservationQuery requires spatial_filter)
so by the time we reach sql_builder we always have one.
"""

from __future__ import annotations

from acropora_store.query_engine import (
    BoundingBox,
    CoverageQuery,
    LocationRadius,
    ObservationQuery,
    SourceQuery,
    SpatialFilter,
)

# Columns returned for observation records
# ingested_at_us is intentionally excluded — internal field not served to consumers
_OBS_COLUMNS = """
    timestamp_us, lat, lon, altitude_m, altitude_type, source_id,
    record_id, node_id, payload_type, payload_schema,
    payload_schema_version, signed_at_us, signature,
    payload, tags
"""


def _build_spatial_condition(
    spatial_filter: SpatialFilter,
    params: list,
    p: int,
) -> tuple[str, int]:
    """
    Build the spatial WHERE condition for a given filter type.
    Appends necessary parameter values to params in place.
    Returns (condition_string, next_param_index).
    """
    if isinstance(spatial_filter, BoundingBox):
        # ST_Within with ST_MakeEnvelope — uses spatial GIST index
        condition = (
            f"ST_Within("
            f"geom, "
            f"ST_MakeEnvelope(${p}, ${p + 1}, ${p + 2}, ${p + 3}, 4326)"
            f")"
        )
        params.extend(
            [
                spatial_filter.lon_min,
                spatial_filter.lat_min,
                spatial_filter.lon_max,
                spatial_filter.lat_max,
            ]
        )
        return condition, p + 4

    elif isinstance(spatial_filter, LocationRadius):
        # ST_DWithin on geography type — uses meters, accurate great-circle distance
        # The ::geography cast enables distance calculation in meters (not degrees)
        condition = (
            f"ST_DWithin("
            f"geom::geography, "
            f"ST_SetSRID(ST_MakePoint(${p}, ${p + 1}), 4326)::geography, "
            f"${p + 2}"
            f")"
        )
        params.extend(
            [
                spatial_filter.lon,
                spatial_filter.lat,
                spatial_filter.radius_m,  # ST_DWithin on geography takes meters
            ]
        )
        return condition, p + 3

    else:
        raise TypeError(f"Unknown spatial filter type: {type(spatial_filter)}")


def build_observation_query(query: ObservationQuery) -> tuple[str, list]:
    """
    Build parameterized SQL for an observation query.
    Returns (sql_string, params_list).
    """
    conditions = [
        "timestamp_us >= $1",
        "timestamp_us < $2",
    ]
    params: list = [
        query.time_range.start_us,
        query.time_range.end_us,
    ]
    p = 3

    # Spatial filter — always present (required by ObservationQuery)
    spatial_condition, p = _build_spatial_condition(query.spatial_filter, params, p)
    conditions.append(spatial_condition)

    # Optional filters
    if query.payload_type is not None:
        conditions.append(f"payload_type = ${p}")
        params.append(query.payload_type)
        p += 1

    if query.source_id is not None:
        conditions.append(f"source_id = ${p}")
        params.append(query.source_id)
        p += 1

    if query.altitude_min_m is not None:
        conditions.append(f"altitude_m >= ${p}")
        params.append(query.altitude_min_m)
        p += 1

    if query.altitude_max_m is not None:
        conditions.append(f"altitude_m <= ${p}")
        params.append(query.altitude_max_m)
        p += 1

    order_dir = "DESC" if query.order == "desc" else "ASC"
    where_clause = " AND ".join(conditions)

    sql = f"""
        SELECT {_OBS_COLUMNS}
        FROM observations
        WHERE {where_clause}
        ORDER BY timestamp_us {order_dir}
        LIMIT ${p}
        OFFSET ${p + 1}
    """
    params.extend([query.limit, query.offset])
    return sql, params


def build_count_query(query: ObservationQuery) -> tuple[str, list]:
    """
    Count query matching the same filters as build_observation_query.
    Strips ORDER BY, LIMIT, OFFSET for efficiency.
    Used to populate total_count in QueryResponse.
    """
    conditions = [
        "timestamp_us >= $1",
        "timestamp_us < $2",
    ]
    params: list = [
        query.time_range.start_us,
        query.time_range.end_us,
    ]
    p = 3

    spatial_condition, p = _build_spatial_condition(query.spatial_filter, params, p)
    conditions.append(spatial_condition)

    if query.payload_type is not None:
        conditions.append(f"payload_type = ${p}")
        params.append(query.payload_type)
        p += 1

    if query.source_id is not None:
        conditions.append(f"source_id = ${p}")
        params.append(query.source_id)
        p += 1

    if query.altitude_min_m is not None:
        conditions.append(f"altitude_m >= ${p}")
        params.append(query.altitude_min_m)
        p += 1

    if query.altitude_max_m is not None:
        conditions.append(f"altitude_m <= ${p}")
        params.append(query.altitude_max_m)
        p += 1

    where_clause = " AND ".join(conditions)
    return f"SELECT count(*) FROM observations WHERE {where_clause}", params


def build_source_query(query: SourceQuery) -> tuple[str, list]:
    """
    Build parameterized SQL for a source track query.
    Returns all observations for a specific source_id within
    the spatial and temporal scope.
    """
    conditions = [
        "source_id = $1",
        "timestamp_us >= $2",
        "timestamp_us < $3",
    ]
    params: list = [
        query.source_id,
        query.time_range.start_us,
        query.time_range.end_us,
    ]
    p = 4

    spatial_condition, p = _build_spatial_condition(query.spatial_filter, params, p)
    conditions.append(spatial_condition)

    order_dir = "DESC" if query.order == "desc" else "ASC"
    where_clause = " AND ".join(conditions)

    sql = f"""
        SELECT {_OBS_COLUMNS}
        FROM observations
        WHERE {where_clause}
        ORDER BY timestamp_us {order_dir}
        LIMIT ${p}
    """
    params.append(query.limit)
    return sql, params


def build_coverage_query(query: CoverageQuery) -> tuple[str, list]:
    """
    Build SQL for a coverage summary query.
    Returns per-payload-type statistics for this node's full dataset.
    No spatial filter — coverage is always reported globally.
    """
    if query.payload_type is not None:
        sql = """
            SELECT
                payload_type,
                count(*)                    AS record_count,
                count(DISTINCT source_id)   AS unique_sources,
                min(timestamp_us)           AS earliest_us,
                max(timestamp_us)           AS latest_us,
                min(lat)                    AS lat_min,
                max(lat)                    AS lat_max,
                min(lon)                    AS lon_min,
                max(lon)                    AS lon_max,
                min(altitude_m)             AS alt_min_m,
                max(altitude_m)             AS alt_max_m
            FROM observations
            WHERE payload_type = $1
            GROUP BY payload_type
        """
        return sql, [query.payload_type]
    else:
        sql = """
            SELECT
                payload_type,
                count(*)                    AS record_count,
                count(DISTINCT source_id)   AS unique_sources,
                min(timestamp_us)           AS earliest_us,
                max(timestamp_us)           AS latest_us,
                min(lat)                    AS lat_min,
                max(lat)                    AS lat_max,
                min(lon)                    AS lon_min,
                max(lon)                    AS lon_max,
                min(altitude_m)             AS alt_min_m,
                max(altitude_m)             AS alt_max_m
            FROM observations
            GROUP BY payload_type
            ORDER BY payload_type
        """
        return sql, []
