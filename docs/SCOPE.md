# Acropora — Project Scope Document

Version: 0.4.0
Date: 2026-03-16
Status: Living document — updated as design decisions are made
PyPI: acropora
Tagline: Run a node. Build the reef.

---

Table of Contents

- [Acropora — Project Scope Document](#acropora--project-scope-document)
  - [1. Project Identity](#1-project-identity)
    - [1.1 Name and Naming System](#11-name-and-naming-system)
    - [1.2 The Three-Level Story](#12-the-three-level-story)
    - [1.3 The Natural Metaphor](#13-the-natural-metaphor)
  - [2. Vision and Mission](#2-vision-and-mission)
  - [3. Problem Statement](#3-problem-statement)
    - [3.1 What Exists Today](#31-what-exists-today)
    - [3.2 What Acropora Fills](#32-what-acropora-fills)
    - [3.3 Technical Description](#33-technical-description)
  - [4. Spatiotemporal Optimization](#4-spatiotemporal-optimization)
    - [4.1 Design Intent](#41-design-intent)
    - [4.2 Source-Centric Queries](#42-source-centric-queries)
  - [5. Ecosystem Overview](#5-ecosystem-overview)
  - [6. Repository Structure](#6-repository-structure)
    - [6.1 Four Repositories](#61-four-repositories)
    - [6.2 Monorepo Internal Structure](#62-monorepo-internal-structure)
    - [6.3 PyPI Package Names](#63-pypi-package-names)
    - [6.4 Package Independence](#64-package-independence)
  - [7. Component Specifications](#7-component-specifications)
    - [7.1 Reef Protocol Specification (`reef-protocol`)](#71-reef-protocol-specification-reef-protocol)
    - [7.2 acropora-ingest](#72-acropora-ingest)
    - [7.3 acropora-store](#73-acropora-store)
    - [7.4 TimescaleDB Schema](#74-timescaledb-schema)
      - [Supporting Tables](#supporting-tables)
      - [Optional Continuous Aggregates (opt-in, default off)](#optional-continuous-aggregates-opt-in-default-off)
    - [7.5 Storage Configuration (operator-configurable)](#75-storage-configuration-operator-configurable)
    - [7.6 acropora-node](#76-acropora-node)
    - [7.7 acropora-client](#77-acropora-client)
    - [7.8 acropora-query (CLI)](#78-acropora-query-cli)
    - [7.9 acropora-registry](#79-acropora-registry)
    - [7.10 Track Registry](#710-track-registry)
    - [7.11 acropora-compose](#711-acropora-compose)
    - [7.12 JavaScript / TypeScript SDK (@acropora/client)](#712-javascript--typescript-sdk-acroporaclient)
  - [8. Privacy Model](#8-privacy-model)
    - [8.1 Core Design Decision](#81-core-design-decision)
    - [8.2 Privacy Dimensions](#82-privacy-dimensions)
    - [8.3 Privacy Presets](#83-privacy-presets)
    - [8.4 Feeder Location Disclosure](#84-feeder-location-disclosure)
  - [9. Query Policy Model](#9-query-policy-model)
    - [9.1 Spatial and Time Requirements](#91-spatial-and-time-requirements)
    - [9.2 Rate Limiting](#92-rate-limiting)
    - [9.3 Bandwidth and Data Volume](#93-bandwidth-and-data-volume)
    - [9.4 Payload Type Restrictions](#94-payload-type-restrictions)
    - [9.5 Consumer Access](#95-consumer-access)
    - [9.6 Optional Aggregate API](#96-optional-aggregate-api)
    - [9.7 Node Visibility to Consumers](#97-node-visibility-to-consumers)
  - [10. Security Model](#10-security-model)
    - [10.2 Defense Layers](#102-defense-layers)
    - [10.3 Database Security](#103-database-security)
    - [10.4 Home Network Protection](#104-home-network-protection)
    - [10.5 Blockchain Relationship](#105-blockchain-relationship)
  - [11.  Data Model](#11--data-model)
    - [11.1 Stored Record Schema v0.4](#111-stored-record-schema-v04)
    - [11.2 Served Record Schema v0.4](#112-served-record-schema-v04)
    - [11.3 Canonical Serialization Algorithm](#113-canonical-serialization-algorithm)
    - [11.4 ADS-B Payload Schema (`acropora/adsb/v1`)](#114-ads-b-payload-schema-acroporaadsbv1)
    - [11.5 Node Advertisement Schema](#115-node-advertisement-schema)
    - [11.6 Reserved Tag Names](#116-reserved-tag-names)
  - [12. Network Architecture](#12-network-architecture)
    - [12.1 Three-Tier Model](#121-three-tier-model)
    - [12.2 Client Fallback Chain](#122-client-fallback-chain)
    - [12.3 Node Discovery Flow](#123-node-discovery-flow)
    - [12.4 Query Decomposition](#124-query-decomposition)
    - [12.5 Private Group Networks](#125-private-group-networks)
    - [12.6 Storage Tiers](#126-storage-tiers)
  - [13. Data Validation and Integrity](#13-data-validation-and-integrity)
    - [13.1 Single-Node Validation (Ingest)](#131-single-node-validation-ingest)
    - [13.2 Cross-Node Byzantine Consensus (Client)](#132-cross-node-byzantine-consensus-client)
    - [13.3 Node Reputation Scoring (Registry)](#133-node-reputation-scoring-registry)
  - [14. Release Roadmap](#14-release-roadmap)
    - [Phase 1 — acropora-store (Single Node, No Networking)](#phase-1--acropora-store-single-node-no-networking)
    - [Phase 2 — acropora-client (Single Node, External Access)](#phase-2--acropora-client-single-node-external-access)
    - [Phase 3 — Reef Protocol Network (Multi-Node Federation)](#phase-3--reef-protocol-network-multi-node-federation)
    - [Phase 4 — Scale + Advanced Features](#phase-4--scale--advanced-features)
  - [15. Component Ownership and Contributor Roles](#15-component-ownership-and-contributor-roles)
    - [15.1 Maintainer Roles](#151-maintainer-roles)
    - [15.2 Governance Principles](#152-governance-principles)
  - [16. Open Questions and Deferred Decisions](#16-open-questions-and-deferred-decisions)
    - [16.1 Naming](#161-naming)
    - [16.2 Technical Decisions](#162-technical-decisions)
    - [16.3 Node-to-Node Request Identity](#163-node-to-node-request-identity)
    - [16.4 Token Incentives](#164-token-incentives)
    - [16.5 Legal and Compliance](#165-legal-and-compliance)
    - [16.6 Governance](#166-governance)
    - [16.7 Track Registry Design](#167-track-registry-design)
    - [16.8 Deferred Features](#168-deferred-features)

## 1. Project Identity
### 1.1 Name and Naming System

| Field | Value |
|---|---|
| Project name | Acropora |
| Protocol | Reef |
| ProtocolNetwork | CORAL network |
| Expansion | Common Observation Repositories, Autonomously Linked |
| Tagline | Run a node. Build the reef. |
| PyPI namespace | `acropora` |
| GitHub org | `github.com/acropora-project` |
| Domain targets | `acropora.network` (primary), `acropora.dev` |
| npm scope | `@acropora` |

### 1.2 The Three-Level Story
```
Acropora nodes
    implement the Reef Protocol
        and collectively form the CORAL network
```
One sentence: *"Acropora is the reference implementation of the Reef Protocol — run a node and join the CORAL network."*

### 1.3 The Natural Metaphor
Acropora is the most species-rich genus of reef-building corals — the branching, fast-growing corals that form the structural backbone of most coral reefs worldwide. The metaphor is precise:

- Acropora nodes are the polyps — independent, locally operated, each archiving its own observations
- The CORAL network is the emergent structure — the sum of all nodes, queryable as a whole
- The reef is what the network builds — a community-owned commons for observation data
- The Reef Protocol is the biology — the rules that make independent polyps interoperable
- An atoll — a coral reef that outlived the volcanic island it formed around — is the governance model. The network persists long after the original founder is gone.

*Common* — the reef is a commons, owned by no single organism.
*Observation* — every polyp observes its local environment
*Repositories* — each node is a self-contained archive of what it has witnessed.
*Autonomously Linked* — polyps operate independently with no central controller, yet link together through shared biology to form a structure greater than any individual.

---

## 2. Vision and Mission

**Vision:** A decentralized, community-owned network for collecting, storing, and sharing spatiotemporal observation data — where no single entity controls the data, privacy is a first-class citizen, and the network survives any single participant including its original author.

**Mission:** To democratize spatiotemporal observation data by providing open-source, self-hostable node software, an open protocol specification, and a suite of tools — enabling hobbyists, enthusiast groups, researchers, and organizations to participate in a federated data commons on their own terms.

**Design Philosophy:**
- Decentralized by default, centralized by choice
- Secure by default, configurable by intent
- **Spatiotemporal-first** — the network is optimized for queries combining a geographic area with a time range; this is a deliberate architectural choice
- Privacy-maximizing out of the box — operators opt in to exposure, never out of protection
- Protocol outlives any maintainer — governance is community-owned
- No single point of failure at any layer
- The data belongs to everyone

---

## 3. Problem Statement

### 3.1 What Exists Today

The ADS-B homelab ecosystem converges on `readsb` and `tar1090` for live display and basic SQLite logging. No open-source, self-hostable, API-first engine exists for long-term spatiotemporal archival, efficient querying, or federated data sharing between independent receivers. The same gap exists for AIS maritime tracking, APRS amateur radio positioning, personal weather stations, air quality sensors, and seismic monitors.

Commercial platforms fill this gap but are proprietary, expensive, and not self-hostable. OpenSky Network is centralized and closed. ADSBexchange aggregates into a central store nobody else can mirror or extend.

### 3.2 What Acropora Fills

| Gap | Acropora Solution |
|---|---|
| No long-term archival pipeline | acropora-store: TimescaleDB + Parquet tiered storage |
| No spatial/temporal query API | acropora-node: FastAPI REST + WebSocket query layer |
| No federated data sharing | Reef Protocol: libp2p gossip + DHT node discovery |
| No privacy controls for operators | Privacy model: configurable per-dimension defaults |
| No cross-node data integrity | Validation: Byzantine consensus + Ed25519 signing |
| No client SDK | acropora-client: Python SDK + CLI tool |
| No open standard | Reef Protocol spec: open, versioned, community-governed |
| No general platform for all data types | Acropora: pluggable schema for any spatiotemporal data |

### 3.3 Technical Description

Acropora is a **federated, decentralized, peer-based network** with gossip-based advertisement propagation and DHT-assisted node discovery, optimized for spatiotemporal observation data. Each node is an independent repository of locally collected observations, cryptographically signed at the source, queryable through a standardized open API, and discoverable through a federated registry system requiring no central authority.

---

## 4. Spatiotemporal Optimization

### 4.1 Design Intent

Acropora is explicitly optimized for spatiotemporal queries — queries that combine a geographic constraint (bounding box or location + radius) with a time range. Every architectural decision is made to serve this query model well:

- Node advertisements describe geographic coverage areas and time ranges — enabling intelligent client routing before any data is transferred
- The query API requires both a spatial filter and a time range — preventing expensive full-network scans
- TimescaleDB hypertables partition by time — enabling efficient chunk exclusion for time-bounded queries
- PostGIS spatial indexes enable fast bounding box and radius filtering within each node
- The client fan-out uses node coverage advertisements to route queries only to relevant nodes — O(relevant nodes) not O(all nodes)
- Parquet files are partitioned by date and payload_type — enabling DuckDB predicate pushdown for historical queries

### 4.2 Source-Centric Queries

Queries scoped to a specific source identifier across a wide geographic area and time range — e.g. "all records for aircraft A1B2C3 on 2025-03-15" covering a LAX→DFW→JFK flight — are not the primary use case and are not optimally served by the federated network alone.

**Why:** Such a query requires data from every node that observed that source — potentially dozens of nodes across a continent with no prior knowledge of which nodes are relevant.

**The track registry solution (Phase 2):** The track registry converts source-centric queries into spatiotemporal queries by maintaining an opt-in index of source_id sightings per node. A client querying for A1B2C3 on 2025-03-15 first consults the track registry to discover which nodes observed that source and their approximate coverage bounding boxes, then fans out standard spatiotemporal queries to only those nodes.

**Great-circle corridor fallback:** When track registry coverage is incomplete, the client estimates the source's ground track by interpolating a great-circle path between known sightings and queries nodes whose coverage overlaps that corridor.

**This is a feature, not a limitation.** Spatiotemporal optimization enables the network to scale efficiently across hundreds of independent nodes without a central coordinator. Applications needing optimized source-centric queries across the full global record should use a data aggregator or the optional central registry.

---

## 5. Ecosystem Overview
```
┌──────────────────────────────────────────────────────────────┐
│                     APPLICATION LAYER                        │
│  AERIS (ADS-B)  │  Future AIS  │  Future APRS  │  Other     │
└───────────────────────────┬──────────────────────────────────┘
                            │ consumes acropora-client
┌───────────────────────────▼──────────────────────────────────┐
│                  ACROPORA CLIENT                             │
│   acropora-client (Python SDK)  │  acropora-query (CLI)     │
│   Discovery · Fan-out · Aggregation · Verification           │
└───────────────────────────┬──────────────────────────────────┘
                            │ queries
┌───────────────────────────▼──────────────────────────────────┐
│            ACROPORA NODE (runs on operator hardware)         │
│                                                              │
│  ┌────────────────────┐     ┌──────────────────────────┐    │
│  │  acropora-ingest   │     │   Reef Protocol Layer    │    │
│  │  acropora-store    │     │   libp2p / FastAPI       │    │
│  └────────────────────┘     └──────────────────────────┘    │
└──────────────────────────────────────────────────────────────┘
                            │ announces to
┌───────────────────────────▼──────────────────────────────────┐
│               acropora-registry (optional)                   │
│   Federated peer registries  │  Optional central registry    │
└──────────────────────────────────────────────────────────────┘
```

**Data flow into a node:**
```
Sensor/Receiver → acropora-ingest adapter → acropora-store → TimescaleDB
```

**Data flow out of a node:**
```
Client query → FastAPI → Query Engine (privacy applied at serve time)
    → TimescaleDB (hot) or DuckDB/Parquet (cold) → signed served record
```

**AERIS ADS-B data flow:**
```
SDR Antenna → readsb → aircraft.json
    → acropora-ingest (readsb adapter) → acropora-store
```

---

## 6. Repository Structure

### 6.1 Four Repositories

| Repo | Contents | License | Rationale |
|---|---|---|---|
| `reef-protocol` | Reef Protocol specification | CC BY 4.0 | Community governance — separate from implementation |
| `acropora` | All Python platform components (monorepo) | MIT | Deeply interdependent during active development |
| `acropora-js` | JavaScript / TypeScript SDK | MIT | Different language, toolchain, release cycle |
| *(future)* `acropora-js` | JS SDK | MIT | Phase 2 — not yet created |

All under `github.com/acropora-project/`.

A `.github` org repo provides shared templates and the org landing page.

### 6.2 Monorepo Internal Structure
```
acropora/
    pyproject.toml              Workspace root (uv)
    README.md
    LICENSE                     MIT
    CHANGELOG.md
    .python-version             3.12
    .gitignore
    .pre-commit-config.yaml

    packages/
        acropora-client/        acropora-client on PyPI
            acropora_client/
                client.py
                discovery.py
                fanout.py
                aggregator.py
                scoring.py
                decomposer.py
                planner.py

        acropora-store/         acropora-store on PyPI
            acropora_store/
                schema.py
                policies.py
                privacy.py
                query_engine.py
                sql_builder.py
                executor.py
                query_router.py
                archival.py
                duckdb_query.py
                aggregates.py

        acropora-ingest/        acropora-ingest on PyPI
            acropora_ingest/
                pipeline.py
                signer.py
                validator.py
                writer.py
                dead_letter.py
                adapters/
                    base.py
                    readsb.py
                    ais.py          (future)
                    aprs.py         (future)
                    weather.py      (future)

        acropora-node/          acropora-node on PyPI
            acropora_node/
                node.py
                api/
                    routes.py
                    models.py
                network/
                    gossip.py
                    dht.py
                    advertisement.py
                    identity.py

        acropora-registry/      acropora-registry on PyPI
            acropora_registry/
                registry.py
                spatial_index.py
                sync.py
                track_registry.py

    tools/
        acropora-query/         acropora-query on PyPI — CLI
        acropora-dashboard/     Node operator web UI

    deploy/
        acropora-compose/
            docker-compose.yml
            .env.example
            config/
            scripts/
                setup.sh
                backup.sh
                health-check.sh
                start.sh
            docs/

    docs/
        SCOPE.md                This document
        architecture.md
        schema.md
        privacy.md
        query-policy.md
        security.md
        spatiotemporal-optimization.md
        contributing.md

    scripts/
        dev-setup.sh
        test-all.sh
        lint-all.sh
```

### 6.3 PyPI Package Names
```
acropora                Meta package
acropora-client         Client SDK
acropora-node           Full node
acropora-store          Storage layer
acropora-ingest         Ingest pipeline + adapters
acropora-registry       Registry component
acropora-query          CLI tool
```

### 6.4 Package Independence

`acropora-client` has zero dependency on server-side packages. A visualization developer installs only `acropora-client` without pulling in TimescaleDB drivers, FastAPI, or asyncpg. This is the critical isolation boundary — the client SDK and node server can version independently.

Dependency graph:
```
acropora (meta)
    └── acropora-node
            ├── acropora-store
            └── acropora-ingest

acropora-client     ← NO dependency on acropora-store or acropora-node
acropora-registry
    └── acropora-node
```

---

## 7. Component Specifications

### 7.1 Reef Protocol Specification (`reef-protocol`)

Community-maintained open document defining the rules all nodes must follow. Storage-agnostic and implementation-agnostic. Anyone may implement it.

**Defines:**
- Node identity format (Ed25519 keypair)
- Node advertisement schema and re-announcement rules
- Gossip propagation rules (TTL, deduplication, re-announcement interval)
- DHT key derivation for geographic routing (geohash-based)
- Query API contract (request/response schemas, error codes)
- Canonical observation record schema and schema extension mechanism
- Privacy metadata format and configuration model
- Query policy model
- Validation metadata format and confidence scoring algorithm
- Node reputation scoring rules
- Protocol version negotiation
- Key rotation procedure
- LADD opt-out propagation
- Track registry (opt-in source sighting index)

**License:** CC BY 4.0
**Governance:** Community GitHub org, maintainer council, BIP/RFC-style numbered proposals

---

### 7.2 acropora-ingest

Ingest pipeline — bridges raw data sources and acropora-store. Source-agnostic via pluggable adapter interface.

**Pipeline stages:**
```
SourceAdapter → Normalizer → Validator → Signer → Writer → TimescaleDB
                                  ↓ (invalid)
                             DeadLetterQueue
```
**SourceAdapter interface** — abstract base all adapters implement. Yields `RawRecord` objects. Handles reconnection internally.

**ReadsbAdapter** — reads readsb `aircraft.json` on configurable poll interval (default 1s). Filters aircraft with no position or stale positions.

**ReadsbNormalizer** — maps readsb fields to `acropora/adsb/v1` canonical schema. Converts feet→meters, strips callsign whitespace, rounds floats.

**RecordValidator** — physical plausibility checks: coordinate range, altitude range, ICAO address format, squawk format, NIC/NACp range, movement speed plausibility via Haversine distance against last known position per source_id.

**RecordSigner** — computes `record_id` (SHA-256 of canonical serialization) and Ed25519 `signature` (covers `record_id + node_id + signed_at_us`).

**RecordWriter** — async batch insert to TimescaleDB. Default batch size 100, flush interval 1s. `ON CONFLICT DO NOTHING` for idempotency.

**DeadLetterQueue** — invalid records written to JSONL file and `dead_letter` table for diagnostics.

---

### 7.3 acropora-store
Storage, query, and archival layer.

**Sub-components:**

`schema.py` — field constants, StorageConfig, PrivacyConfig, QueryPolicyConfig
`policies.py` — idempotent TimescaleDB compression and retention policy management
`privacy.py` — serve_record() — applies privacy transformations at query time
`query_engine.py` — translates Reef Protocol query requests to SQL
`sql_builder.py` — parameterized SQL construction for all query types
`executor.py` — async query execution against TimescaleDB
`query_router.py` — routes queries to hot (TimescaleDB) or cold (DuckDB/Parquet) tier
`aggregates.py` — optional continuous aggregate management (disabled by default)
`archival.py` — nightly Parquet export pipeline
`duckdb_query.py` — DuckDB query engine over Parquet archive

### 7.4 TimescaleDB Schema
**Observations Hypertable**

```sql
CREATE TABLE observations (
    -- Spatiotemporal core (raw, full precision)
    timestamp_us            BIGINT              NOT NULL,
    lat                     DOUBLE PRECISION    NOT NULL,
    lon                     DOUBLE PRECISION    NOT NULL,
    altitude_m              REAL,
    altitude_type           TEXT,
    source_id               TEXT,

    -- Provenance
    record_id               CHAR(64)            NOT NULL,
    node_id                 TEXT                NOT NULL,
    payload_type            TEXT                NOT NULL,
    payload_schema          TEXT                NOT NULL,
    payload_schema_version  TEXT                NOT NULL,
    ingested_at_us          BIGINT              NOT NULL,
    signed_at_us            BIGINT              NOT NULL,
    signature               TEXT                NOT NULL,

    -- Payload (raw JSONB)
    payload                 JSONB               NOT NULL DEFAULT '{}',

    -- Tags
    tags                    JSONB,

    -- Derived geometry (auto-computed)
    geom                    GEOMETRY(Point, 4326)
                            GENERATED ALWAYS AS (
                                ST_SetSRID(ST_MakePoint(lon, lat), 4326)
                            ) STORED,

    CONSTRAINT valid_lat        CHECK (lat BETWEEN -90 AND 90),
    CONSTRAINT valid_lon        CHECK (lon BETWEEN -180 AND 180),
    CONSTRAINT valid_record_id  CHECK (length(record_id) = 64),
    CONSTRAINT valid_type       CHECK (payload_type IN (
                                    'adsb','ais','aprs',
                                    'weather','seismic','custom'
                                ))
);
```

**Hypertable:** partitioned by `timestamp_us`, chunk interval 1 day.
**Compression:** `segmentby = 'source_id, payload_type'`, compress after 7 days (configurable).

#### Supporting Tables
```
node_config         Node identity, privacy config, storage config, query policy
ingest_stats        Daily counters per payload_type
dead_letter         Invalid records — auto-purged after 30 days
archival_log        Parquet export history — chunk, path, record count, checksum
track_sightings     Opt-in source sighting index for track registry
```

#### Optional Continuous Aggregates (opt-in, default off)
```
obs_density_1h_01deg    Hourly density by 0.1° grid
obs_density_1d_01deg    Daily density by 0.1° grid
obs_source_tracks_1h    Hourly track summary per source
```

---

### 7.5 Storage Configuration (operator-configurable)
```yaml
yamlstorage:
  hot_retention_days: 90
  compress_after_days: 7
  archive_enabled: true
  archive_after_days: 85
  archive_path: "/var/lib/acropora/archive"
  archive_compression: "zstd"
  archive_target_file_size_mb: 256
  aggregates:
    enabled: false
    hourly_density_enabled: false
    daily_density_enabled: false
    track_summary_enabled: false
    expose_density_api: false
```

**Validation rules:** `compress_after_days` < `hot_retention_days`; `archive_after_days` < `hot_retention_days`; `compress_after_days` ≤ `archive_after_days`.

**Query performance with compression:**

| Query type | Data age | Hits | Typical speed |
|---|---|---|---|
| Single source, recent | < 7 days | Uncompressed | Milliseconds |
| Single source, older | > 7 days | Compressed, single segment | 10–100ms |
| Heatmap (aggregates on) | Any | Continuous aggregate | Milliseconds |
| Heatmap (aggregates off) | Any | Raw observations | Seconds |
| Bulk analytics | > 90 days | Parquet + DuckDB | 1–10 seconds |

---

### 7.6 acropora-node

Complete node package — acropora-ingest + acropora-store + Reef Protocol network layer + dashboard.

**Network layer:**
- Node identity (Ed25519 keypair, node_id derivation, key rotation)
- Node advertisement (signed coverage + privacy summary + schema list)
- GossipSub via libp2p
- Kademlia DHT via libp2p
- FastAPI query API (REST + WebSocket, TLS required)

---

### 7.7 acropora-client

Python SDK abstracting node discovery, fan-out, aggregation, and verification.

**Responsibilities:** Node discovery via fallback chain, query planning, spatial + temporal decomposition, parallel async fan-out, response aggregation and deduplication, Byzantine consensus validation, Ed25519 signature verification, coverage gap detection, local advertisement cache.

**Client fallback chain:**
```
Central registry → Federated registries → DHT → Local cache → Direct URL → Local-only
```

**Query planner:** Scores nodes by sub-queries needed (prefer fewer), reputation, and data freshness. Decomposes large spatial queries into grid tiles and large time ranges into chunks fitting within each node's `max_time_range_hours`. A node allowing 5-day windows scores ~3× higher than a node limited to 24-hour windows for a 3-day query.

---

### 7.8 acropora-query (CLI)
```bash
acropora query \
  --bbox "32.5,-98.5,33.5,-96.5" \
  --last-hours 24 \
  --payload-type adsb \
  --output parquet > results.parquet

acropora query \
  --location "32.9,-97.0" \
  --radius-km 100 \
  --last-hours 6

acropora node status
acropora node rotate-key
acropora discover --bbox "32.5,-98.5,33.5,-96.5"
acropora export --format parquet --start "2026-01-01" --output ./export/
```

---

### 7.9 acropora-registry
**Federated registry (anyone can run):** Lightweight node extension maintaining spatial index of known advertisements. Serves GET /nodes?bbox=.... Syncs with peer registries. Holds routing metadata only.

**Central registry (optional, operator-run):** Aggregates all public federated registries. Pre-builds global spatial index. Pre-computes query routing. Caches pre-aggregated tiles. Publishes aggregated index as open dataset. Never required for network function.

---

### 7.10 Track Registry
Opt-in index of source_id sightings. Enables source-centric queries to be efficiently decomposed into spatiotemporal queries. See Section 4.2 for full description.
**Track sighting record:**
```yaml
source_id: string
payload_type: string
node_id: string
bbox: [lat_min, lon_min, lat_max, lon_max]   # quantized per node's coordinate_dp
first_seen_us: int64
last_seen_us: int64
observation_count: int32
signed_at_us: int64
signature: string
```
**Node advertisement capability:**
```yaml
capabilities:
  publishes_track_sightings: false    # DEFAULT — opt-in
  track_sighting_retention_days: 30
  track_sighting_delay_minutes: 0
```

---

### 7.11 acropora-compose
Docker Compose stack. Installs and configures readsb, TimescaleDB, acropora-node, and acropora-dashboard in a single command. Three readsb modes:

- `bundled` — acropora-compose starts its own readsb (default for fresh installs)
- `native` — readsb already running on host, mount its output directory
- `external` — readsb running in another Docker container, mount its volume

Setup wizard auto-detects existing readsb and configures the correct mode. PostgreSQL port never published to host network — internal Docker network only.

---

### 7.12 JavaScript / TypeScript SDK (@acropora/client)
Browser- and Node.js-compatible SDK implementing discovery, fan-out, aggregation, and verification. Required for browser-based visualization applications. Phase 2 deliverable.

## 8. Privacy Model
**Guiding principle:** Privacy-maximizing by default. Operators opt in to exposure.

### 8.1 Core Design Decision
Raw full-precision observations are stored locally. Privacy transformations are applied at query time based on current operator settings. Stored records are never modified.

Database-level access is prevented through network isolation (PostgreSQL bound to localhost, never published to host network) rather than encryption at rest.

### 8.2 Privacy Dimensions
**Dimension 1 — Location Privacy**
```yaml
bbox_mode: "snapped"           # snapped|fuzzy|region|exact|none
bbox_snap_degrees: 1.0
bbox_fuzz_km: 0
exclusion_zone_km: 25
exclusion_lat: null
exclusion_lon: null
disclose_exclusion_zone: false
```
**Dimension 2 — Data Precision**
```yaml
coordinate_dp: 2               # 1=~11km 2=~1.1km 3=~110m 4=~11m 5=exact
altitude_precision: "100ft"    # 10000ft|1000ft|100ft|exact|stripped
rssi_mode: "strip"             # strip|bucketed|quantized|full
vector_precision: "low"        # low|medium|full
include_raw_messages: false
```
**Dimension 3 — Access Control**
```yaml
access_mode: "public"          # public|key_required|allowlist|private
discoverable: true
```
**Dimension 4 — Temporal Control**
```yaml
embargo_minutes: 0
retention_days: 365
honor_ladd_requests: true
persist_on_exit: false
```

### 8.3 Privacy Presets

| Preset | Use Case | Key Settings |
|---|---|---|
| `maximum_privacy` | Maximum protection | region bbox, 50km exclusion, key_required, 60min embargo |
| `private_contributor` | **DEFAULT** home operators | 1° snapped, 25km exclusion, strip RSSI, 2dp, public |
| `community_feeder` | Enthusiast groups | 0.5° snapped, 10km exclusion, bucketed RSSI, 3dp |
| `organization` | Airports, institutions | exact bbox, no exclusion, quantized RSSI, 4dp |
| `research` | Maximum fidelity | exact bbox, no exclusion, full RSSI, 5dp, raw messages |

### 8.4 Feeder Location Disclosure

Physical receiver location cannot be fully protected when serving observation data. Acknowledged attack vectors: bounding box centroid inference, coverage shape analysis, RSSI distance inference, multi-node triangulation, timing analysis. Plain-language disclosure shown before node goes live.

---

## 9. Query Policy Model

### 9.1 Spatial and Time Requirements

Both a spatial filter and a time range are **always required** for all observation queries. These requirements are not operator-configurable — they are protocol-level constraints protecting both the individual node and the network.

```
Required: time range (start+end or last_hours)
Required: spatial filter (bbox OR location+radius_km)
```
Error response when missing:
```json
{
  "error": "spatial_filter_required",
  "message": "A spatial filter is required. Queries without a spatial
              constraint would scan all locations — expensive for the
              node and for the network.",
  "options": [
    {"option": "bbox", "example": "bbox=32.5,-98.5,33.5,-96.5"},
    {"option": "location+radius_km", "example": "location=32.9,-97.0&radius_km=100"}
  ]
}
```
### 9.2 Rate Limiting
```yaml
rate_limiting:
  enabled: true
  requests_per_minute: 60
  burst_allowance: 10
  max_concurrent_queries: 5
```
### 9.3 Bandwidth and Data Volume
```yaml
bandwidth:
  max_records_per_query: 10000
  max_time_range_hours: 24
  max_bbox_degrees: 5.0
  max_radius_km: 500
```
### 9.4 Payload Type Restrictions
```yaml
allowed_payload_types:
  - adsb
  # Operator can restrict to specific types
```
### 9.5 Consumer Access
```yaml
access_control:
  mode: "public"
  blocked_keys: []
  blocked_ips: []
```
### 9.6 Optional Aggregate API
```yaml
aggregate_api:
  enabled: false    # Only if aggregates.enabled = true
  # Exposes GET /aggregate/density
```
### 9.7 Node Visibility to Consumers
Current: node can see requester IP, API key, query parameters, request volume.
Planned (Phase 3 open question): node-to-node request identity via signed request headers.

---

## 10. Security Model
10.1 Threat Model
|Threat|Description|
|---|---|
|Impersonation|Attacker pretends to be a legitimate node|
|Transit tampering|Data modified between node and client|
|Data injection|Node deliberately serves fabricated data|
|Enumeration|Attacker discovers all node IP addresses|
|Resource exhaustion|Node overwhelmed by expensive queries|
|Key compromise|Node's private key stolen|
|Sybil attack|Attacker creates many fake nodes|
|Supply chain|Malicious code in node software|

### 10.2 Defense Layers
|Layer|Technique|Defends Against|Version|
|---|---|---|---|
|1|TLS everywhere|Transit tampering|v1|
|2|Ed25519 record signing at ingest|Impersonation, post-transit tampering|v1|
|3|Byzantine cross-node consensus|Malicious node data injection|v2|
|4|Public key transparency log|Key compromise, unauthorized rotation|v3|
|5|Merkle proofs|Historical data tampering|v3|
|6|MLAT cross-validation|GPS spoofing at source|v4|
|7|Web of trust endorsement|Sybil attacks|v2|

### 10.3 Database Security
PostgreSQL bound to localhost only. Port never published in acropora-compose. Internal Docker network. Minimal database role — SELECT, INSERT on observations only. Unix socket connections preferred.

### 10.4 Home Network Protection
- Bind to specific interfaces only — never `0.0.0.0`
- Run as non-root user
- Docker container with no host network access
- Relay tunnel option for nodes without open inbound ports
- Signed releases with checksums
- No undisclosed outbound connections

### 10.5 Blockchain Relationship
Acropora independently implements the valuable cryptographic primitives that make blockchains useful — Ed25519 keypairs, content-addressed records (SHA-256 record_id), tamper-evident signatures, append-only transparency log (planned v3), Merkle proofs (planned v3), distributed consensus (Byzantine fault detection) — without the properties that would harm the project: prohibitive storage costs, query performance limitations, privacy conflicts with transparent public ledgers, and confirmation latency incompatible with real-time ingestion.

---

## 11.  Data Model
### 11.1 Stored Record Schema v0.4
Raw, full-precision. Never served directly to consumers.
```yaml
# Layer 1 — Spatiotemporal core (raw)
timestamp_us: int64             # Microseconds UTC — when source emitted
lat: float64                    # Full precision WGS84
lon: float64                    # Full precision WGS84
altitude_m: float32 | null      # Meters above WGS84 ellipsoid
altitude_type: string | null    # geometric|barometric|pressure|agl
source_id: string | null        # Emitter identity per payload_type
                                # adsb: ICAO hex  ais: MMSI  aprs: callsign

# Layer 2 — Provenance
record_id: string               # SHA-256 of canonical serialization (64 hex)
node_id: string                 # SHA-256 of Ed25519 public key bytes
payload_type: string            # adsb|ais|aprs|weather|seismic|custom
payload_schema: string          # e.g. acropora/adsb/v1
payload_schema_version: string  # e.g. 1.0.0
ingested_at_us: int64           # When node ingested (vs when source emitted)
signed_at_us: int64             # When node signed
signature: string               # Base64url Ed25519
                                # covers: record_id + node_id + signed_at_us

# Layer 3 — Payload (raw, schema-specific)
payload: object

# Layer 4 — Tags
tags: [[string]] | null
```
### 11.2 Served Record Schema v0.4
Built at query time by `serve_record()`. Privacy transformations applied from current `PrivacyConfig`. Identical to stored record except:

- `lat/lon` quantized to `coordinate_dp` decimal places
- `altitude_m` quantized to `altitude_precision`
- `payload`.rssi_dbm null if `rssi_mode = strip`
- `ingested_at_us` not included (internal)
- `privacy` object added (generated at serve time)
- Records within exclusion zone not served
- Records within embargo window not served

```yaml
# Added at serve time
privacy:
  coordinate_dp: int8
  altitude_precision: string
  rssi_stripped: bool
  embargo_minutes: int16
  exclusion_zone_km: int16
  settings_version: string      # Hash of current privacy config
```

### 11.3 Canonical Serialization Algorithm

**For `record_id`:**
1. Build serialization object with fields in layer definition order (not alphabetical)
2. Serialize to JSON — no whitespace, UTF-8, null values included explicitly
3. `record_id = SHA-256(canonical_bytes).hexdigest()`

**For `signature`:**
```
signing_input = record_id + ":" + node_id + ":" + str(signed_at_us)
signature = base64url(ed25519_sign(signing_input))
```
### 11.4 ADS-B Payload Schema (`acropora/adsb/v1`)
```yaml
callsign: string | null         # ICAO flight ID, 8 chars max
squawk: string | null           # 4 octal digits
altitude_baro_ft: int32 | null  # Barometric altitude in feet
ground_speed_kt: float | null   # Ground speed in knots
track_deg: float | null         # True track 0–359°
vertical_rate_fpm: int32 | null # ft/min — positive = climbing
nic: int8 | null                # Navigation Integrity Category 0–11
nac_p: int8 | null              # Navigation Accuracy Category 0–9
category: string | null         # A0–D7
rssi_dbm: float | null          # Raw RSSI — null if rssi_mode = strip
messages_seen: int32 | null     # Mode S messages this ingest cycle
```
### 11.5 Node Advertisement Schema
```yaml
node_id: string
public_key: string
protocol_version: string
supported_payload_types:
  - payload_type: string
    payload_schema: string
    payload_schema_version: string
    record_count: int
    earliest_data: string
    latest_data: string
    available_fields: [string]
    privacy_notes:
      rssi_available: bool
      coordinate_dp: int
      altitude_precision: string
endpoint: string | null
relay: string | null
coverage:
  bbox: [lat_min, lon_min, lat_max, lon_max]
  alt_range_m: [min, max]
  earliest_data: string
  latest_data: string
privacy_summary:
  preset: string
  has_exclusion_zone: bool
  rssi_available: bool
  coordinate_precision: int
  embargo_minutes: int
  access_mode: string
query_policy_summary:
  max_records_per_query: int
  max_time_range_hours: float
  max_bbox_degrees: float
  max_radius_km: float
  requires_api_key: bool
capabilities:
  publishes_track_sightings: false
  track_sighting_retention_days: 30
  track_sighting_delay_minutes: 0
reputation:
  agreement_rate: float
  uptime_30d: float
  days_in_network: int
signed_at: string
signature: string
```

### 11.6 Reserved Tag Names

| Tag | Format | Meaning |
|---|---|---|
| `ref` | `["ref", "{record_id}"]` | Cross-reference to another record |
| `source-software` | `["source-software", "{name}", "{version}"]` | Software that produced raw data |
| `quality` | `["quality", "{tier}"]` | verified\|unverified\|suspect |
| `collection` | `["collection", "{name}"]` | Named collection or campaign |
| `antenna` | `["antenna", "{type}", "{gain_dbi}"]` | Antenna metadata |
| `receiver` | `["receiver", "{type}", "{model}"]` | Receiver hardware |

Custom tags must use `x-` prefix.

---

## 12. Network Architecture

### 12.1 Three-Tier Model
```
Tier 0 — Direct
  Node A → Node B via known URL or overlay (Tailscale etc.)
  No dependencies. Always works if both nodes are online.

Tier 1 — Federated
  Client → Registry → Nodes
  Anyone can run a registry.
  Works as long as any registry is online.

Tier 2 — Central (optional)
  Client → Hosted central registry
  Pre-built global spatial index. Fastest discovery.
  Single point of failure — Tier 1 fallback is automatic.
  Never required for network function.
```

### 12.2 Client Fallback Chain
```
Central registry → Federated registries → DHT → Flooding query
    → Local advertisement cache → Local-only mode
```

### 12.3 Node Discovery Flow

1. Compute geohash for target bounding box
2. Query registry or DHT
3. Receive node advertisements
4. Verify Ed25519 signatures
5. Filter by supported schemas, data policy, query policy
6. Rank by coverage overlap, data freshness, reputation, sub-queries needed
7. Fan out queries in parallel
8. Aggregate, deduplicate, validate
9. Cache advertisement list locally with TTL

### 12.4 Query Decomposition

When a client's query exceeds a node's constraints, the query planner decomposes it:

**Temporal decomposition:** A 3-day query against a node limited to 24-hour windows is split into 3 sequential sub-queries. A node allowing 5-day windows scores ~3× higher and is preferred.

**Spatial decomposition:** A 500km radius query against a node limited to 50km radius is decomposed into a grid of overlapping circles. Results are deduplicated by `record_id`.

### 12.5 Private Group Networks

Groups configure `discoverable: false`, announce only to private registry, use `access_mode: allowlist`. Invisible to public CORAL network. Same software, different configuration. Can federate with the public network later.

### 12.6 Storage Tiers

| Tier | Storage | Retention | Query Engine |
|---|---|---|---|
| Hot | TimescaleDB | 0–90 days (configurable) | PostgreSQL + PostGIS |
| Warm | Parquet on local disk | 90 days–2 years | DuckDB |
| Cold | Compressed Parquet, object storage | 2+ years | DuckDB |

---

## 13. Data Validation and Integrity

### 13.1 Single-Node Validation (Ingest)

| Check | Logic |
|---|---|
| Coordinate range | lat ∈ [-90,90], lon ∈ [-180,180] |
| Altitude plausibility | altitude_m ∈ [-11000, 100000] |
| Position movement | Haversine ÷ elapsed time ≤ 1100 m/s |
| Vertical rate | altitude delta ÷ elapsed time ≤ 50 m/s |
| ICAO format | 6 hex chars for ADS-B |
| Squawk format | 4 octal digits |
| NIC range | 0–11 |
| Timestamp sanity | Within ±30s of system clock |
| Duplicate suppression | Identical messages within 100ms window |

### 13.2 Cross-Node Byzantine Consensus (Client)

Applied when 3+ nodes return overlapping data for same source_id in 30-second window. Computes median position, calculates per-node deviation, assigns confidence tier.

| Tier | Condition |
|---|---|
| `high` | 3+ nodes agree, deviation < 500m |
| `medium` | 3+ nodes agree, deviation 500m–2000m |
| `low` | 2 nodes agree or deviation 2000m–threshold |
| `suspect` | Deviation exceeds threshold |
| `unverified` | Single node — cannot validate |

### 13.3 Node Reputation Scoring (Registry)

Tracked over rolling 30-day window: `agreement_rate`, `outlier_rate`, `uptime_30d`, `data_freshness_ms`, `coverage_consistency`. Published in node advertisement.

---

## 14. Release Roadmap

### Phase 1 — acropora-store (Single Node, No Networking)

*Goal: Working local observation archive with queryable REST API.*

| # | Component | Status |
|---|---|---|
| 1.1 | Canonical schema v0.4 | ✅ Complete |
| 1.2 | acropora-ingest — ADS-B adapter | ✅ Complete |
| 1.3 | TimescaleDB schema + policies | ✅ Complete |
| 1.4 | Single-node validation | ✅ Complete |
| 1.5 | Privacy transformation layer | ✅ Complete |
| 1.6 | Query engine — FastAPI REST + spatial requirement | ✅ Complete |
| 1.7 | Node Ed25519 keypair + signing | ✅ Complete |
| 1.8 | acropora-compose Docker stack | ✅ Complete |
| 1.9 | Parquet archival pipeline + DuckDB | ✅ Complete |

*Milestone: Single node ingesting readsb, queryable via REST, data signed at ingest, Parquet archival running.*

---

### Phase 2 — acropora-client (Single Node, External Access)

*Goal: Application developers can query a known node. SDK usable before federation exists.*

| # | Component | Priority |
|---|---|---|
| 2.1 | OpenAPI 3.0 spec published | Critical |
| 2.2 | acropora-client Python SDK — single node | Critical |
| 2.3 | acropora-query CLI | High |
| 2.4 | acropora-dashboard node UI | High |
| 2.5 | Sandbox test node with synthetic data | High |
| 2.6 | Track registry — opt-in source sighting index | High |
| 2.7 | acropora-client source query support | High |
| 2.8 | Track registry lookup + spatiotemporal decomposition | |
| 2.9 | Great-circle corridor fallback for gaps | |
| 2.10 | @acropora/client JS SDK — single node | High |

*Milestone: Visualization apps (AERIS etc.) working against a real node. Developer workflow established.*

---

### Phase 3 — Reef Protocol Network (Multi-Node Federation)

*Goal: Nodes discover each other, acropora-client fans out across the CORAL network.*

| # | Component | Priority |
|---|---|---|
| 3.1 | Protocol version negotiation | Critical |
| 3.2 | libp2p — GossipSub | Critical |
| 3.3 | Node advertisement + gossip propagation | Critical |
| 3.4 | Kademlia DHT | Critical |
| 3.5 | acropora-registry — federated mode | High |
| 3.6 | acropora-client fan-out + aggregation | Critical |
| 3.7 | Byzantine consensus validation | High |
| 3.8 | Node reputation scoring | High |
| 3.9 | Key rotation mechanism | Critical |
| 3.10 | LADD compliance — opt-out propagation | High |
| 3.11 | Web of trust endorsement | Medium |
| 3.12 | @acropora/client fan-out + federation | High |
| 3.13 | Network health dashboard | Medium |
| 3.14 | Prometheus metrics endpoint | Medium |

*Milestone: Multiple nodes federated. acropora-client discovers and queries across the CORAL network.*

---

### Phase 4 — Scale + Advanced Features

*Goal: Central registry, auditability, second data type.*

| # | Component | Priority |
|---|---|---|
| 4.1 | Central registry — hosted aggregator | High |
| 4.2 | Public key transparency log | High |
| 4.3 | Safe auto-update mechanism | High |
| 4.4 | Backup and restore tooling | Medium |
| 4.5 | Merkle proof support | Medium |
| 4.6 | BIP/RFC protocol proposal process | Medium |
| 4.7 | MLAT cross-validation | Low |
| 4.8 | Second data type — AIS or APRS | Medium |

---

## 15. Component Ownership and Contributor Roles

### 15.1 Maintainer Roles

| Role | Responsibilities |
|---|---|
| Protocol Maintainer | reef-protocol spec, RFC process, breaking change ratification |
| Store Maintainer | acropora-ingest, acropora-store, TimescaleDB, Parquet |
| Network Maintainer | libp2p, gossip, DHT, advertisement layer |
| Client Maintainer | acropora-client, acropora-query, @acropora/client |
| Infrastructure Maintainer | acropora-compose, CI/CD, release signing, dashboard |
| Registry Operator | Community federated registry + optional central registry |

### 15.2 Governance Principles

- reef-protocol spec lives in community org — not personal account
- No single maintainer ratifies breaking protocol changes unilaterally
- All software MIT licensed
- Central registry clearly identified as optional operator-run infrastructure
- Central registry aggregated index published as open dataset

---

## 16. Open Questions and Deferred Decisions

### 16.1 Naming

| Question | Status | Notes |
|---|---|---|
| GitHub org — `acropora` username taken by inactive user | Open | Try creating org `acropora` directly (user vs org may coexist). Submit GitHub support request. Fallback: `acropora-project`. |
| Domain `acropora.org` | Open | Parked — contact registrant to purchase. Primary domain is `acropora.network`. |
| npm `@acropora` scope | Check needed | Confirm available on npmjs.com |

### 16.2 Technical Decisions

| Question | Status | Notes |
|---|---|---|
| Wire format: MessagePack vs JSON | Open | MessagePack ~30–40% more compact. Decide before Reef Protocol spec finalized. |
| Geohash precision for DHT keys | Open | Needs empirical testing with simulated network sizes. |
| Gossip re-announcement interval | Open | 30 minutes proposed — validate against convergence simulations. |
| TimescaleDB vs QuestDB | Open | TimescaleDB recommended. QuestDB has better raw ingest throughput. Decide in Phase 1 prototyping. |
| Relay infrastructure model | Open | Cloudflare Tunnel vs frp vs custom relay. |
| Bootstrap node hosting | Open | Need 3+ geographically distributed stable nodes before public launch. |
| Protocol versioning scheme | Open | Semver: major = breaking, minor = additive, patch = clarification. |
| LADD opt-out list source | Open | FAA doesn't publish machine-readable list. Community-maintained list proposed. Legal review needed. |

### 16.3 Node-to-Node Request Identity

**Status: Open — Reef Protocol spec decision**

When an Acropora node queries another node as part of client fan-out, should it identify itself with a signed request header?
```
X-Acropora-Node-Id: {node_id}
X-Acropora-Node-Sig: base64url(sign(request_hash + timestamp_us))
```
Potential benefits: receiving node can log peer queries, apply differential rate limits, build peer network topology. Potential concerns: reduces query anonymity, adds per-request overhead.

### 16.4 Token Incentives
**Status: Deferred — post-Phase 4**
A token incentive model could reward node operators for uptime, data quality, geographic coverage, and query serving. Key questions before designing: Does tokenization conflict with the open commons philosophy? Which ledger technology is appropriate? How are quality and coverage verified without central authority? What prevents gaming? Legal and regulatory implications?

This is explicitly post-Phase 4. The platform must prove value and grow community before introducing economic incentives.

### 16.5 Legal and Compliance
| Question | Status | Notes |
|---|---|---|
|Terms of service | Open | Needed before public launch |
|GDPR for EU nodes | Open | Legal review needed |
|LADD compliance | Open | honor_ladd_requests: true default pending legal review |
|License for downstream apps | Open | MIT confirmed for platform. Apps may warrant review if commercial use anticipated. |

### 16.6 Governance
| Question | Status | Notes |
|---|---|---|
| GitHub org name | Open | acropora vs acropora-project — see 16.1 |
| Initial maintainer council | Open | Solo initially — define community governance threshold |
| Protocol RFC tooling | Open | GitHub Discussions vs dedicated RFC repo |
| Trademark / domain registration | Open | Consider protecting Acropora, CORAL, Reef Protocol |

### 16.7 Track Registry Design
| Question | Status | Notes |
|---|---|---|
| Sighting granularity| Open | Per node per aircraft per hour? Per flight leg? Per ingest session? |
| Corridor width heuristic | Open | 200km default proposed. Configurable per payload_type? |
| Central registry source index | Open | Should central registry maintain a global source index independent of node publication? |
| Track registry federation | Open | Can track registries sync sighting indexes with each other? |

### 16.8 Deferred Features
| Feature | Deferred To | Reason |
|---|---|---|
| MLAT cross-validation | Phase 4+ | Raw timing data sharing — significant protocol complexity |
| Merkle proof support | Phase 4 | Not critical for v1 trust model |
| Web of trust endorsement | Phase 3 | Ed25519 signing sufficient initially |
| Second data type (AIS/APRS) | Phase 4 | ADS-B as reference implementation first |
| Distributed tracing| Phase 3+ | Useful for debugging, not launch-blocking |
|Data jurisdiction tooling | Post-launch | Documentation first |
| Token incentive model | Post-Phase 4 | Platform must prove value first |
| Continuous aggregates (default) | Operator opt-in | Resolution flexibility more valuable than pre-computation |
|acropora-client extraction to own repo | Phase 3/4 | Extract when v1.0 stable + external contributors arrive |

*Document version 0.4.0 — 2026-03-16
Supersedes version 0.3.1-draft (Coral Reef)
Project renamed from Coral Reef to Acropora. Protocol renamed to Reef Protocol.
CORAL retained as network name (Common Observation Repositories, Autonomously Linked).*