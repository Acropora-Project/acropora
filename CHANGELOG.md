# Changelog

## [0.1.0] - Unreleased

### Added

#### Monorepo & Tooling

- Root `pyproject.toml` with workspace configuration and shared dev dependencies
- `uv.lock` for reproducible installs across all packages
- `.python-version` pinning
- Pre-commit config (`.pre-commit-config.yaml`) with ruff and mypy hooks
- Dev, lint, and test helper scripts (`scripts/dev-setup.sh`, `scripts/lint-all.sh`, `scripts/test-all.sh`)
- Root `conftest.py` with `collect_ignore_glob` to exclude non-test packages from pytest discovery
- Coverage configuration in root `pyproject.toml`

#### CI/CD

- GitHub Actions CI workflow (`.github/workflows/ci.yml`): lint, type-check, test matrix
- GitHub Actions release workflow (`.github/workflows/release.yml`)

#### acropora-store

- TimescaleDB-backed schema with hypertable and compression configuration (`schema.py`)
- DuckDB query engine for archival data with spatial and temporal filtering (`duckdb_query.py`)
- SQL builder for dynamic query construction (`sql_builder.py`)
- Query engine with fan-out across live and archival data sources (`query_engine.py`)
- Query policy enforcement and access control (`query_policy.py`, `policies.py`)
- Privacy layer with anonymization and suppression rules (`privacy.py`)
- Query router for dispatching to live vs. archival backends (`query_router.py`)
- Archival pipeline for moving aged data to Parquet storage (`archival.py`)
- Query executor orchestrating policy, privacy, and routing (`executor.py`)
- Aggregate helper types (`aggregates.py`)
- Comprehensive test suite: privacy, query engine, query policy, and schema modules

#### acropora-ingest

- Package scaffold with `pyproject.toml`, typed marker, and test structure

#### acropora-node

- Package scaffold with `pyproject.toml`, typed marker, and test structure

#### acropora-client

- Package scaffold with `pyproject.toml`, typed marker, and test structure

#### acropora-registry

- Package scaffold with `pyproject.toml`, typed marker, and test structure

#### acropora-dashboard (tool)

- Tool scaffold with `pyproject.toml` and test structure

#### acropora-query (tool)

- Tool scaffold with `pyproject.toml` and test structure

#### Deploy

- Docker Compose stack (`deploy/acropora-compose/docker-compose.yml`) with readsb mode detection
- PostgreSQL/TimescaleDB init SQL and `postgresql.conf` tuning
- `.env.example` documenting all required environment variables

#### Docs

- `docs/SCOPE.md` — full protocol and architecture scope document
- Per-package `README.md` files for store, ingest, node, client, registry, dashboard, and query
- Updated root `README.md`
- `LICENSE`
