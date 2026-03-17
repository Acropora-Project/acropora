# acropora-dashboard

Local operator web UI for Acropora nodes.

Shows node health, storage usage, coverage map, ingest statistics,
query rate, active peers, and privacy configuration. Bound to
localhost only — never accessible externally.

Included in the acropora-compose Docker stack. Also runnable
standalone:

```bash
pip install acropora-dashboard
acropora-dashboard --node-url http://localhost:5000
```

## Part of Acropora

Part of the [Acropora](https://github.com/acropora-project/acropora)
platform.
