# acropora-ingest

Data ingest pipeline for Acropora nodes.

Reads raw sensor output, normalizes to the canonical
observation schema, validates for physical plausibility,
signs with the node's Ed25519 keypair, and writes to
the acropora-store TimescaleDB instance.

## Supported adapters

- **readsb** — ADS-B via readsb `aircraft.json` output
- AIS (planned)
- APRS (planned)
- Weather (planned)

## Part of Acropora

Part of the [Acropora](https://github.com/acropora-project/acropora)
platform.