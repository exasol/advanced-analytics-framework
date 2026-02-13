# Developer Environment

## Installation

### Project Requirements

For this project you need

- Lua 5.4
- Luarocks 3.*
- Python 3.10
- Poetry

### Install on a desktop machine

1. Install poetry following the instructions on their [website](https://python-poetry.org/docs/).
2. Install micromamba as explained on their [website](https://mamba.readthedocs.io/en/latest/installation/micromamba-installation.html)
3. To install lua and all luarocks package, run from the project root 

    ```bash
    ./scripts/install_conda_packages.sh
    ./scripts/install_luarocks_packages.sh
    ```
   
### Install in Vagrant and CI

From the project root run:

1. `./scripts/install_lua_environment.sh`
2. Install poetry 
   - Already installed in Vagrant

## Usage

### Interactive

From the project root run:

   ```bash
   source ./scripts/activate_development_environment.sh
   ```

### Use in scripts and CI

From the project root run:

   ```bash
   source ./scripts/run_in_dev_env.sh <command>
   ```
