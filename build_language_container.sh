#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(dirname "$(readlink -f "${BASH_SOURCE[0]}")")"
pushd  $SCRIPT_DIR &> /dev/null
poetry build

FLAVOR_NAME=exasol_advanced_analytics_framework_container
echo "$FLAVOR_NAME"
FLAVOR_PATH="language_container/$FLAVOR_NAME"
RELEASE_BUILD_STEP_DIST_DIRECTORY="$FLAVOR_PATH/flavor_base/release/dist"
echo "Copy" dist/*.whl "$RELEASE_BUILD_STEP_DIST_DIRECTORY"
mkdir -p "$RELEASE_BUILD_STEP_DIST_DIRECTORY" || true
cp dist/*.whl "$RELEASE_BUILD_STEP_DIST_DIRECTORY"



TEST_PACKAGE_SCRIPT_DIR="$SCRIPT_DIR/tests/test_package"
echo "$TEST_PACKAGE_SCRIPT_DIR"
pushd  $TEST_PACKAGE_SCRIPT_DIR &> /dev/null
poetry build
popd
RELEASE_BUILD_STEP_DIST_DIRECTORY="$FLAVOR_PATH/flavor_base/test/dist"
echo "Copy" dist/*.whl "$RELEASE_BUILD_STEP_DIST_DIRECTORY"
mkdir -p "$RELEASE_BUILD_STEP_DIST_DIRECTORY" || true
cp dist/*.whl "$RELEASE_BUILD_STEP_DIST_DIRECTORY"



echo "Build container"
./language_container/exaslct export --flavor-path "$FLAVOR_PATH" --release-goal test

echo "Generate language activation"
./language_container/exaslct generate-language-activation --flavor-path "$FLAVOR_PATH" --bucketfs-name bfsdefault --bucket-name default --path-in-bucket container --container-name "$FLAVOR_NAME" 
