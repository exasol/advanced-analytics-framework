#!/bin/bash

set -euo pipefail

GIT_REF=main
CHECKOUT_PATH=/tmp/integration-test-docker-environment

if [ -d "$CHECKOUT_PATH" ]
then
  cd "$CHECKOUT_PATH"
  git checkout "$GIT_REF"
  git pull
else
  git clone https://github.com/exasol/integration-test-docker-environment.git "$CHECKOUT_PATH"
  cd "$CHECKOUT_PATH"
  git checkout "$GIT_REF"
fi


./start-test-env spawn-test-environment --environment-name test --database-port-forward 8888 --bucketfs-port-forward 6666 --db-mem-size 4GB --nameserver 8.8.8.8
