#!/bin/bash -i

SCRIPT_DIR="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"

source "$SCRIPT_DIR"/activate_development_environment.sh

echo "Running following command inside the development environment:" "${@}"

"${@}"