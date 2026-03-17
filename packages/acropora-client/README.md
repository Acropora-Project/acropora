# acropora-client

Python client SDK for the CORAL network.

Query any Acropora node — or the full federated network — with
a single async call. Handles node discovery, spatial and temporal
query decomposition, parallel fan-out, result aggregation,
deduplication, and Ed25519 signature verification transparently.
```python
from acropora_client import AcroporaClient, BoundingBox, TimeRange

client = AcroporaClient(
    registries=["https://registry.acropora.network"],
)

result = await client.query(
    spatial_filter=BoundingBox(32.5, -98.5, 33.5, -96.5),
    time_range=TimeRange.last_hours(24),
    payload_type="adsb",
)

print(f"{result.total_count} records from {len(result.nodes_queried)} nodes")
```

## Installation
```bash
pip install acropora-client
```

## Part of Acropora

Part of the [Acropora](https://github.com/acropora-project/acropora)
platform — the reference implementation of the
[Reef Protocol](https://github.com/acropora-project/reef-protocol).