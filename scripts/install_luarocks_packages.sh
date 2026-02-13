#!/bin/bash -i
set -euo pipefail
SCRIPT_DIR="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"

$SCRIPT_DIR/run_in_dev_env.sh luarocks install --only-deps advanced-analytics-framework-*.rockspec
