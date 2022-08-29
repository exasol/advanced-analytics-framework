#!/bin/bash -i
set -euo pipefail

micromamba create -y --prefix ./.conda_env -f conda.yml
