# acropora-node

Full Acropora node — run this to join the CORAL network.

Bundles acropora-ingest (data ingestion) and acropora-store
(TimescaleDB + Parquet storage) with the Reef Protocol
network layer (libp2p gossip + DHT) and a FastAPI query API.

## Quick start
```bash
cd deploy/acropora-compose
./scripts/setup.sh
```

Or directly:
```bash
pip install acropora-node
acropora node start
```

## Part of Acropora

Part of the [Acropora](https://github.com/acropora-project/acropora)
platform.