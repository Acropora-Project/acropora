#!/usr/bin/env bash
# Install all packages in development mode

set -euo pipefail

if ! command -v uv &> /dev/null; then
    echo "Installing uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
fi

echo "Installing all packages in development mode..."
uv sync --all-packages

echo ""
echo "✓ Development environment ready"
echo ""
echo "Run tests:   uv run pytest"
echo "Run linter:  uv run ruff check ."
echo "Type check:  uv run mypy packages/ tools/"