#!/bin/bash

micromamba activate --prefix ./.conda_env
readonly export_lua_path_file=$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")
luarocks_tree_path="$export_lua_path_file"/../.luarocks
eval "$(luarocks --tree "$luarocks_tree_path"  path)"
export PATH="$luarocks_tree_path/bin:$PATH"
poetry env use python3.8