#!/bin/bash
set -euo pipefail

cd $HOME
mkdir bin || true
# See https://github.com/exasol/advanced-analytics-framework/issues/184
# wget -qO- https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba
# wget -qO- https://micro.mamba.pm/api/micromamba/linux-64/1.5.10 | tar -xvj bin/micromamba
wget -qO- https://micro.mamba.pm/api/micromamba/linux-64/2.0.0 | tar -xvj bin/micromamba
./bin/micromamba shell init -s bash --root-prefix ~/micromamba

echo 'export PATH="$HOME/bin:$PATH"' >>"$HOME"/.bashrc
export PATH="$HOME/bin:$PATH"
