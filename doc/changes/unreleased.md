# Unreleased

## Summary

This release fixes vulnerabilities by updating dependencies:

| Dependency   | Affected | Vulnerability       | Fixed in | Updated to |
|--------------|----------|---------------------|----------|------------|
| black        | 25.12.0  | CVE-2026-32274      | 26.3.1   | 2.3.1      |
| cryptography | 46.0.5   | CVE-2026-34073      | 46.0.6   | 46.0.6     |
| pyasn1       | 0.6.2    | CVE-2026-30922      | 0.6.3    | 0.6.3      |
| pygments     | 2.19.2   | CVE-2026-4539       | 2.20.0   | 2.20.0     |
| requests     | 2.32.5   | CVE-2026-25645      | 2.33.0   | 2.33.1     |
| tornado      | 6.5.4    | GHSA-78cv-mqj4-43f7 | 6.5.5    | 6.5.5      |
| tornado      | 6.5.4    | CVE-2026-31958      | 6.5.5    | 6.5.5      |

## Security Issues

* #345: Fixed vulnerabilities by updating dependencies

## Refactorings

* #339: Updated to exasol-toolbox version `6.0.0`
* #341: Removed formatting overrides
