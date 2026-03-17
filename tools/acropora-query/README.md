# acropora-query

CLI tool for querying the CORAL network.

```bash
pip install acropora-query

acropora query \
  --bbox "32.5,-98.5,33.5,-96.5" \
  --last-hours 24 \
  --payload-type adsb

acropora node status
acropora discover --bbox "32.5,-98.5,33.5,-96.5"
```

Also installed as `acro` for brevity:

```bash
acro query --location "32.9,-97.0" --radius-km 100 --last-hours 6
```

## Part of Acropora

Part of the [Acropora](https://github.com/acropora-project/acropora)
platform.
