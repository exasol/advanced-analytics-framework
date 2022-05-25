#!/usr/bin/env bash

set -eo pipefail

# set release goal, set to test by default
RELEASE_GOAL="--release-goal test"
if [ ! -z $1 ]
then
  if [ "$1" = "release" ]
  then
    RELEASE_GOAL=""
  else
    echo "Invalid release goal: $1"
    exit
  fi
fi



# main package - release
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


# test package - test
if [ ! "$1" = "release" ]
then
  TEST_PACKAGE_SCRIPT_DIR="$SCRIPT_DIR/tests/test_package"
  pushd  $TEST_PACKAGE_SCRIPT_DIR &> /dev/null
  poetry build

  RELEASE_BUILD_STEP_DIST_DIRECTORY="$SCRIPT_DIR/$FLAVOR_PATH/flavor_base/test/dist"
  echo "Copy" dist/*.whl "$RELEASE_BUILD_STEP_DIST_DIRECTORY"
  mkdir -p "$RELEASE_BUILD_STEP_DIST_DIRECTORY" || true
  cp dist/*.whl "$RELEASE_BUILD_STEP_DIST_DIRECTORY"


  # switch to main directory
  popd
fi


# build container
echo "Build container"
./language_container/exaslct export --flavor-path "$FLAVOR_PATH" $RELEASE_GOAL

echo "Generate language activation"
./language_container/exaslct generate-language-activation --flavor-path "$FLAVOR_PATH" --bucketfs-name bfsdefault --bucket-name default --path-in-bucket container --container-name "$FLAVOR_NAME"
