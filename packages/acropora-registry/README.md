# acropora-registry

Federated node discovery registry for the CORAL network.

Run a registry to help nodes and clients find each other.
Maintains a spatial index of node advertisements and serves
`GET /nodes?bbox=...` discovery queries.

Anyone can run a registry — see
[network architecture](https://github.com/acropora-project/reef-protocol/blob/main/spec/09-network-protocol.md)
for the federated registry model.

## Part of Acropora

Part of the [Acropora](https://github.com/acropora-project/acropora)
platform.