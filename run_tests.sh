#!/bin/bash

set -euo pipefail

project_name=$(basename $(readlink -f $(dirname $0)))

poetry run pytest
poetry run ruff check
poetry run mypy . --show-error-codes
poetry run bandit -r . -x ./tests/
poetry run pylint $project_name
poetry run pylint tests

echo "SUCCESS!"

