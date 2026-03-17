# Acropora

A federated, decentralized platform for spatiotemporal observation data.

**Run a node. Build the reef.**

Acropora nodes implement the [Reef Protocol](https://github.com/acropora-project/reef-protocol)
and collectively form the **CORAL network** —
*Common Observation Repositories, Autonomously Linked*.

## Quick start
```bash
git clone https://github.com/acropora-project/acropora
cd acropora/deploy/acropora-compose
./scripts/setup.sh
```

## Packages

| Package | PyPI | Description |
|---|---|---|
| `acropora-node` | [acropora-node](https://pypi.org/project/acropora-node) | Full node — run this to join the network |
| `acropora-client` | [acropora-client](https://pypi.org/project/acropora-client) | Client SDK — query the network from your app |
| `acropora-store` | [acropora-store](https://pypi.org/project/acropora-store) | Storage layer — TimescaleDB + Parquet |
| `acropora-ingest` | [acropora-ingest](https://pypi.org/project/acropora-ingest) | Ingest pipeline + adapters |
| `acropora-registry` | [acropora-registry](https://pypi.org/project/acropora-registry) | Registry component |
| `acropora-query` | [acropora-query](https://pypi.org/project/acropora-query) | CLI tool |

## Status

🚧 Pre-alpha — Phase 1 development in progress

## Links

- [acropora.network](https://acropora.network)
- [Protocol spec](https://github.com/acropora-project/reef-protocol)
- [Scope document](docs/SCOPE.md)