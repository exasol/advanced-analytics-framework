#!/bin/bash

set -euo pipefail

# ./scripts/run_in_dev_env.sh poetry run itde spawn-test-environment --environment-name test --database-port-forward 9563 --bucketfs-port-forward 6666 --db-mem-size 4GB --nameserver 8.8.8.8
./scripts/run_in_dev_env.sh \
    poetry run itde spawn-test-environment \
    --environment-name test \
    --database-port-forward 8888 \
    --bucketfs-port-forward 6666 \
    --db-mem-size 4GB \
    --nameserver 8.8.8.8
