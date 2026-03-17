#!/usr/bin/env bash
set -euo pipefail
uv run pytest --tb=short "$@"