# 0.6.2 - 2025-10-29

## Summary

This release updates the dependency declaration to `pyexasol` allowing versions `>=0.25.0,<2` which required updating some additional dependency declarations:

| Dependency | old               | new           |
|------------|-------------------|---------------|
| PyExasol   | `>=0.25.0,<1.0.0` | `>=0.25.0,<2` |
| ITDE       | `^3.4.0`          | `>=3.4.0,<5`  |
| SLCT       | `^1.1.0`          | `>1,<4`       |
| PYTBE      | `>=0.3.0,<1.0.0`  | `>=0.3.0,<2`  |

* ITDE: exasol-integration-test-docker-environment
* SLCT: exasol-script-languages-container-tool
* PYTBE: pytest-exasol-backend

However the release restricts dependency `structlog` to versions `<25.5` as version 25.5 makes many integration tests without database fail.

## Refactorings

* Updated locked dependencies & reformatted files
* #314: Renamed test directories
* #254: Re-Enabled coverage reporting in Github workflows
* #319: Ignored errors reported by Python tool `coverage`
* #321: Updated dependency declaration to pyexasol

## Dependency Updates

### `main`
* Updated dependency `click:8.2.1` to `8.3.0`
* Updated dependency `exasol-bucketfs:1.1.0` to `2.1.0`
* Updated dependency `joblib:1.5.1` to `1.5.2`
* Updated dependency `nox:2025.5.1` to `2025.10.16`
* Updated dependency `pandas:2.3.1` to `2.3.3`
* Updated dependency `pydantic:2.11.7` to `2.12.3`
* Updated dependency `pyexasol:0.27.0` to `1.2.0`

### `dev`
* Updated dependency `elasticsearch:8.18.1` to `8.19.2`
* Updated dependency `exasol-integration-test-docker-environment:3.4.0` to `4.3.0`
* Updated dependency `exasol-python-extension-common:0.10.0` to `0.12.0`
* Updated dependency `exasol-script-languages-container-tool:1.1.0` to `3.4.1`
* Updated dependency `exasol-toolbox:1.6.1` to `1.12.0`
* Updated dependency `exasol-udf-mock-python:0.4.0` to `0.5.0`
* Added dependency `pandas-stubs:2.3.2.250926`
* Updated dependency `polyfactory:2.22.1` to `2.22.3`
* Updated dependency `pytest:7.4.4` to `8.4.2`
* Updated dependency `pytest-exasol-backend:0.4.0` to `1.2.2`
* Updated dependency `pytest-exasol-extension:0.2.3` to `0.2.4`
* Updated dependency `pytest-exasol-slc:0.3.2` to `0.3.0`
* Added dependency `types-networkx:3.5.0.20251001`
