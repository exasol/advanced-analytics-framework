#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"

$SCRIPT_DIR/run_in_dev_env.sh luarocks --tree .luarocks install advanced-analytics-framework-*.rockspec
