# 0.10.0 - 2026-11-20

## Summary

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| black | CVE-2026-114134 | 26.5.1 | 26.10.1 |
| certifi | PYSEC-2026-4320 | 2026.7.22 | 2026.10.18 |
| filelock | CVE-2026-112907 | 4.0.1 | 4.2.1 |
| filelock | CVE-2026-112908 | 4.0.1 | 4.2.1 |
| filelock | CVE-2026-112909 | 4.0.1 | 4.2.1 |
| filelock | CVE-2026-112910 | 4.0.1 | 4.2.1 |
| filelock | CVE-2026-112911 | 4.0.1 | 4.2.1 |
| filelock | CVE-2026-112912 | 4.0.1 | 4.2.1 |
| filelock | CVE-2026-112913 | 4.0.1 | 4.2.1 |
| filelock | CVE-2026-112915 | 4.0.1 | 4.2.1 |
| filelock | CVE-2026-112934 | 4.0.1 | 4.2.1 |
| filelock | CVE-2026-112935 | 4.0.1 | 4.2.1 |
| filelock | CVE-2026-112936 | 4.0.1 | 4.0.9 |
| filelock | CVE-2026-112938 | 4.0.1 | 4.2.1 |
| filelock | CVE-2026-112939 | 4.0.1 | 4.2.1 |
| filelock | CVE-2026-112940 | 4.0.1 | 4.2.1 |
| gitpython | CVE-2026-110855 | 3.1.62 | 3.2.0 |
| gitpython | CVE-2026-110856 | 3.1.62 | 3.2.0 |
| gitpython | CVE-2026-110857 | 3.1.62 | 3.2.0 |
| gitpython | CVE-2026-110858 | 3.1.62 | 3.2.0 |
| gitpython | CVE-2026-110859 | 3.1.62 | 3.2.0 |
| gitpython | CVE-2026-114145 | 3.1.62 | 3.2.1 |
| msgpack | CVE-2026-114613 | 1.2.2 | 1.2.3 |
| pylint | CVE-2026-114608 | 4.0.8 | 4.0.9 |
| soupsieve | CVE-2026-126190 | 2.9.2 | 4.0.0 |
| soupsieve | CVE-2026-126191 | 2.9.2 | 3.0.2 |
| soupsieve | CVE-2026-126192 | 2.9.2 | 2.10.0 |
| soupsieve | CVE-2026-126193 | 2.9.2 | 3.0.2 |
| tornado | CVE-2026-115927 | 6.5.10 | 6.6 |
| tornado | CVE-2026-116577 | 6.5.10 | 6.6 |
| tornado | CVE-2026-116578 | 6.5.10 | 6.6 |
| tornado | CVE-2026-126347 | 6.5.10 | 6.6.1 |
| virtualenv | PYSEC-2026-4372 | 21.11.0 | 21.14.10 |
| virtualenv | PYSEC-2026-4373 | 21.11.0 | 21.14.9 |
| virtualenv | PYSEC-2026-4374 | 21.11.0 | 21.14.9 |
| virtualenv | PYSEC-2026-4375 | 21.11.0 | 21.14.9 |
| virtualenv | PYSEC-2026-4376 | 21.11.0 | 21.14.9 |
| virtualenv | PYSEC-2026-4377 | 21.11.0 | 21.14.9 |
| virtualenv | PYSEC-2026-4378 | 21.11.0 | 21.14.9 |
| virtualenv | PYSEC-2026-4379 | 21.11.0 | 21.14.9 |
| virtualenv | PYSEC-2026-4380 | 21.11.0 | 21.14.9 |
| virtualenv | PYSEC-2026-4381 | 21.11.0 | 21.14.9 |
| virtualenv | PYSEC-2026-4382 | 21.11.0 | 21.14.9 |
| virtualenv | PYSEC-2026-4383 | 21.11.0 | 21.14.9 |
| virtualenv | PYSEC-2026-4384 | 21.11.0 | 21.14.7 |
| virtualenv | PYSEC-2026-4385 | 21.11.0 | 21.14.4 |
| virtualenv | PYSEC-2026-4386 | 21.11.0 | 21.14.2 |
| virtualenv | PYSEC-2026-4387 | 21.11.0 | 21.14.2 |

* #377: Fixed vulnerabilities by updating dependencies

## Bugfixes

* #379: Fixed hash function

## Refactorings

* #374: Updated to PTB 12.0.0 and dropped support for Python 3.10

## Dependency Updates

### `main`

* Updated dependency `click:8.5.0` to `8.5.1`
* Updated dependency `exasol-bucketfs:2.3.0` to `2.4.0`
* Updated dependency `pydantic:2.13.5` to `2.14.3`

### `dev`

* Updated dependency `exasol-script-languages-container-tool:4.4.0` to `4.5.0`
* Updated dependency `exasol-toolbox:10.5.0` to `12.0.0`
* Updated dependency `pytest:9.1.1` to `9.2.0`
* Updated dependency `types-networkx:3.6.1.20260911` to `3.6.1.20261119`
