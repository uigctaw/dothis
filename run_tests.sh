#!/bin/bash

set -euo pipefail

poetry install

poetry run ruff check --fix
poetry run pytest
poetry run mypy . --show-error-codes --check-untyped

echo "SUCCESS!"

