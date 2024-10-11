# Developer Guide

The developer guide explains how you can build this project.

* [developer_environment](developer_environment.md)

## Build the SLC

If you want to build the AAF SLC then

1. Download the [AAF Sources](https://github.com/exasol/advanced-analytics-framework/) from GitHub
2. Install poetry
3. Use the AAF developer commands for building and deploying custom SLCs into Exasol.

```shell
poetry run nox -s build_language_container
```

See [Install the SLC](../user_guide/user_guide.md#install-the-slc).
