# 0.9.0 - 2026-09-23

## Summary

This release fixes vulnerabilities by updating dependencies.

## Security Issues

This release fixes vulnerabilities by updating dependencies:

| Dependency | Vulnerability | Affected | Fixed in |
|------------|---------------|----------|----------|
| anyio | CVE-2026-63374 | 4.12.1 | 4.14.2 |
| anyio | CVE-2026-64847 | 4.12.1 | 4.14.2 |
| black | PYSEC-2026-2121 | 25.12.0 | 26.3.1 |
| black | PYSEC-2026-2121 | 25.12.0 | 26.3.1 |
| black | PYSEC-2026-2120 | 25.12.0 | 26.3.0 |
| click | PYSEC-2026-2132 | 8.2.1 | 8.3.3 |
| cryptography | PYSEC-2026-35 | 46.0.5 | 46.0.6 |
| cryptography | PYSEC-2026-36 | 46.0.5 | 46.0.7 |
| cryptography | PYSEC-2026-36 | 46.0.5 | 46.0.7 |
| cryptography | PYSEC-2026-35 | 46.0.5 | 46.0.6 |
| cryptography | PYSEC-2026-3554 | 46.0.5 | 49.0.0 |
| cryptography | PYSEC-2026-3552 | 46.0.5 | 50.0.0 |
| cryptography | PYSEC-2026-3553 | 46.0.5 | 49.0.0 |
| cryptography | PYSEC-2026-3552 | 46.0.5 | 50.0.0 |
| cryptography | PYSEC-2026-3553 | 46.0.5 | 49.0.0 |
| cryptography | PYSEC-2026-3554 | 46.0.5 | 49.0.0 |
| cryptography | GHSA-537c-gmf6-5ccf | 46.0.5 | 48.0.1 |
| gitpython | PYSEC-2026-2160 | 3.1.46 | 3.1.47 |
| gitpython | PYSEC-2026-2161 | 3.1.46 | 3.1.47 |
| gitpython | PYSEC-2026-2163 | 3.1.46 | 3.1.49 |
| gitpython | PYSEC-2026-3984 | 3.1.46 | 3.1.60 |
| gitpython | PYSEC-2026-3980 | 3.1.46 | 3.1.50 |
| gitpython | PYSEC-2026-3982 | 3.1.46 | 3.1.60 |
| gitpython | PYSEC-2026-3981 | 3.1.46 | 3.1.53 |
| gitpython | PYSEC-2026-2161 | 3.1.46 | 3.1.47 |
| gitpython | PYSEC-2026-2160 | 3.1.46 | 3.1.47 |
| gitpython | PYSEC-2026-2162 | 3.1.46 | 3.1.48 |
| gitpython | PYSEC-2026-2163 | 3.1.46 | 3.1.49 |
| gitpython | PYSEC-2026-3783 | 3.1.46 | 3.1.58 |
| gitpython | PYSEC-2026-3785 | 3.1.46 | 3.1.59 |
| gitpython | PYSEC-2026-3786 | 3.1.46 | 3.1.59 |
| gitpython | PYSEC-2026-3783 | 3.1.46 | 3.1.58 |
| gitpython | PYSEC-2026-3787 | 3.1.46 | 3.1.59 |
| gitpython | PYSEC-2026-3788 | 3.1.46 | 3.1.59 |
| gitpython | PYSEC-2026-3784 | 3.1.46 | 3.1.58 |
| gitpython | PYSEC-2026-3786 | 3.1.46 | 3.1.59 |
| gitpython | PYSEC-2026-3787 | 3.1.46 | 3.1.59 |
| gitpython | PYSEC-2026-3788 | 3.1.46 | 3.1.59 |
| gitpython | PYSEC-2026-3840 | 3.1.46 | 3.1.58 |
| gitpython | PYSEC-2026-3836 | 3.1.46 | 3.1.51 |
| gitpython | PYSEC-2026-3838 | 3.1.46 | 3.1.58 |
| gitpython | PYSEC-2026-3839 | 3.1.46 | 3.1.51 |
| gitpython | PYSEC-2026-3841 | 3.1.46 | 3.1.58 |
| gitpython | PYSEC-2026-3837 | 3.1.46 | 3.1.59 |
| gitpython | PYSEC-2026-3843 | 3.1.46 | 3.1.58 |
| gitpython | PYSEC-2026-3949 | 3.1.46 | 3.1.57 |
| gitpython | PYSEC-2026-3951 | 3.1.46 | 3.1.55 |
| gitpython | PYSEC-2026-3953 | 3.1.46 | 3.1.54 |
| gitpython | PYSEC-2026-3952 | 3.1.46 | 3.1.54 |
| gitpython | PYSEC-2026-3950 | 3.1.46 | 3.1.56 |
| gitpython | PYSEC-2026-3948 | 3.1.46 | 3.1.57 |
| gitpython | CVE-2026-73624 | 3.1.46 | 3.1.54 |
| idna | PYSEC-2026-215 | 3.11 | 3.15 |
| idna | PYSEC-2026-215 | 3.11 | 3.15 |
| msgpack | PYSEC-2026-3625 | 1.1.2 | 1.2.1 |
| msgpack | PYSEC-2026-3625 | 1.1.2 | 1.2.1 |
| pip | PYSEC-2026-2875 | 26.0.1 | 26.1 |
| pip | PYSEC-2026-2876 | 26.0.1 | 26.1 |
| pip | PYSEC-2026-196 | 26.0.1 | 26.1.2 |
| pip | PYSEC-2026-196 | 26.0.1 | 26.1.2 |
| pip | PYSEC-2026-2875 | 26.0.1 | 26.1 |
| pip | PYSEC-2026-2876 | 26.0.1 | 26.1 |
| pip | PYSEC-2026-3721 | 26.0.1 | 26.2 |
| pip | PYSEC-2026-3721 | 26.0.1 | 26.2.0 |
| pyasn1 | PYSEC-2026-2263 | 0.6.2 | 0.6.3 |
| pyasn1 | PYSEC-2026-2263 | 0.6.2 | 0.6.3 |
| pyasn1 | PYSEC-2026-3456 | 0.6.2 | 0.6.4 |
| pyasn1 | PYSEC-2026-3457 | 0.6.2 | 0.6.4 |
| pyasn1 | PYSEC-2026-3457 | 0.6.2 | 0.6.4 |
| pyasn1 | PYSEC-2026-3456 | 0.6.2 | 0.6.4 |
| pyasn1 | PYSEC-2026-3455 | 0.6.2 | 0.6.4 |
| pyasn1 | PYSEC-2026-3455 | 0.6.2 | 0.6.4 |
| pygments | PYSEC-2026-2987 | 2.19.2 | 2.20.0 |
| pygments | PYSEC-2026-2987 | 2.19.2 | 2.20.0 |
| pytest | PYSEC-2026-1845 | 8.4.2 | 9.0.3 |
| pytest | PYSEC-2026-1845 | 8.4.2 | 9.0.3 |
| requests | PYSEC-2026-2275 | 2.32.5 | 2.33.0 |
| requests | PYSEC-2026-2275 | 2.32.5 | 2.33.0 |
| soupsieve | PYSEC-2026-3072 | 2.8.3 | 2.8.4 |
| soupsieve | PYSEC-2026-3071 | 2.8.3 | 2.8.4 |
| soupsieve | PYSEC-2026-3072 | 2.8.3 | 2.8.4 |
| soupsieve | PYSEC-2026-3071 | 2.8.3 | 2.8.4 |
| soupsieve | CVE-2026-85999 | 2.8.3 | 2.9.0 |
| soupsieve | CVE-2026-86000 | 2.8.3 | 2.9.0 |
| starlette | PYSEC-2026-161 | 0.52.1 | 1.0.1 |
| starlette | PYSEC-2026-161 | 0.52.1 | 1.0.1 |
| starlette | PYSEC-2026-2281 | 0.52.1 | 1.1.0 |
| starlette | PYSEC-2026-2280 | 0.52.1 | 1.1.0 |
| starlette | PYSEC-2026-249 | 0.52.1 | 1.3.1 |
| starlette | PYSEC-2026-248 | 0.52.1 | 1.3.0 |
| starlette | PYSEC-2026-249 | 0.52.1 | 1.3.1 |
| starlette | PYSEC-2026-248 | 0.52.1 | 1.3.0 |
| starlette | PYSEC-2026-2281 | 0.52.1 | 1.1.0 |
| starlette | PYSEC-2026-2280 | 0.52.1 | 1.1.0 |
| tornado | PYSEC-2026-2287 | 6.5.4 | 6.5.5 |
| tornado | PYSEC-2026-140 | 6.5.4 | 6.5.5 |
| tornado | PYSEC-2026-2287 | 6.5.4 | 6.5.5 |
| tornado | PYSEC-2026-140 | 6.5.4 | 6.5.5 |
| tornado | PYSEC-2026-3388 | 6.5.4 | 6.5.6 |
| tornado | PYSEC-2026-3387 | 6.5.4 | 6.5.6 |
| tornado | PYSEC-2026-3389 | 6.5.4 | 6.5.6 |
| tornado | PYSEC-2026-2287 | 6.5.4 | 6.5.5 |
| tornado | PYSEC-2026-3387 | 6.5.4 | 6.5.6 |
| tornado | PYSEC-2026-3388 | 6.5.4 | 6.5.6 |
| tornado | PYSEC-2026-3389 | 6.5.4 | 6.5.6 |
| tornado | PYSEC-2026-3928 | 6.5.4 | 6.5.8 |
| tornado | GHSA-pw6j-qg29-8w7f | 6.5.4 | 6.5.7 |
| tornado | GHSA-8423-8fgw-73vq | 6.5.4 | 6.5.8 |
| urllib3 | PYSEC-2026-141 | 2.6.3 | 2.7.0 |
| urllib3 | PYSEC-2026-142 | 2.6.3 | 2.7.0 |
| urllib3 | PYSEC-2026-142 | 2.6.3 | 2.7.0 |
| urllib3 | PYSEC-2026-141 | 2.6.3 | 2.7.0 |

* #345: Fixed vulnerabilities by updating dependencies
* #349: Fixed vulnerabilities by updating dependencies
* #351: Fixed vulnerabilities by updating pytest

## Refactorings

* #339: Updated to exasol-toolbox version `6.0.0`
* #341: Removed formatting overrides
* #342: Fixed building documentation
* #351: Updated to exasol-toolbox version `8.1.1`
* #353: Re-enables `check-workflows` in `checks.yml` and updated to exasol-toolbox version `10.0.0`

## Dependency Updates

### `main`

* Updated dependency `click:8.2.1` to `8.5.0`
* Updated dependency `exasol-bucketfs:2.1.0` to `2.3.0`
* Updated dependency `joblib:1.5.3` to `1.6.0`
* Updated dependency `pydantic:2.12.5` to `2.13.5`
* Updated dependency `pyexasol:1.3.0` to `2.4.1`
* Updated dependency `typeguard:4.4.4` to `4.6.0`

### `dev`

* Updated dependency `exasol-integration-test-docker-environment:5.0.0` to `6.5.1`
* Updated dependency `exasol-python-extension-common:0.12.1` to `0.16.0`
* Updated dependency `exasol-script-languages-container-tool:3.6.1` to `4.4.0`
* Updated dependency `exasol-toolbox:1.13.0` to `10.5.0`
* Updated dependency `pytest:8.4.2` to `9.1.1`
* Updated dependency `pytest-exasol-backend:1.2.5` to `1.5.1`
* Updated dependency `pytest-exasol-extension:0.2.5` to `1.0.1`
* Updated dependency `pytest-exasol-slc:0.4.4` to `1.1.1`
* Updated dependency `types-networkx:3.6.1.20260210` to `3.6.1.20260911`
