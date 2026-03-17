# acropora-store

Storage layer for Acropora nodes.

Provides TimescaleDB schema management, the privacy transformation
layer (applied at query time — raw data is never modified), the
tiered query engine (hot tier via TimescaleDB, cold tier via
DuckDB + Parquet), and the nightly Parquet archival pipeline.

## Storage tiers

| Tier | Storage | Default retention | Query engine |
|---|---|---|---|
| Hot | TimescaleDB | 90 days | PostgreSQL + PostGIS |
| Warm | Parquet on disk | 90 days–2 years | DuckDB |
| Cold | Compressed Parquet | 2+ years | DuckDB |

## Part of Acropora

Part of the [Acropora](https://github.com/acropora-project/acropora)
platform — the reference implementation of the
[Reef Protocol](https://github.com/acropora-project/reef-protocol).