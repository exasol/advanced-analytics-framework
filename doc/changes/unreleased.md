# Unreleased

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

## Refactorings

* Updated locked dependencies & reformatted files
* #314: Renamed test directories
* #254: Re-Enabled coverage reporting in Github workflows
* #319: Ignored errors reported by Python tool `coverage`
* #321: Updated dependency declaration to pyexasol
