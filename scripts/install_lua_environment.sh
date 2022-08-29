#!/bin/bash
set -euo pipefail
SCRIPT_DIR="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"

"$SCRIPT_DIR"/install_micromamba.sh
"$SCRIPT_DIR"/install_conda_packages.sh
"$SCRIPT_DIR"/install_luarocks_packages.sh