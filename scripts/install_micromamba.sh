#!/bin/bash
set -euo pipefail

cd $HOME
mkdir bin || true
wget -qO- https://micro.mamba.pm/api/micromamba/linux-64/2.0.0 | tar -xvj bin/micromamba
./bin/micromamba shell init -s bash --root-prefix ~/micromamba

echo 'export PATH="$HOME/bin:$PATH"' >>"$HOME"/.bashrc
export PATH="$HOME/bin:$PATH"
