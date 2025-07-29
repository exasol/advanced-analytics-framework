# Developer Guide

The developer guide explains how to maintain and develop the Advanced Analytics Framework (AAF).

* [developer_environment](developer_environment.md)

## Building and Installing the AAF Script Language Container (SLC)

The following command builds the SLC for the AAF

```shell
poetry run -- nox -s slc:build
```

GitHub workflow `build-and-publish.yml` also adds the SLC to each release of the AAF on GitHub.

Installing the SLC ins described in the [AAF User Guide](../user_guide/user_guide.md#script-language-container-slc).

## Update Generated Files

AAF contains the amalgated Lua script [create_query_loop.sql](https://github.com/exasol/advanced-analytics-framework/blob/main/exasol/analytics/resources/outputs/create_query_loop.sql) originating from the files in the directory [exasol/analytics/lua/src](https://github.com/exasol/advanced-analytics-framework/blob/main/exasol/analytics/lua/src/).

The following command updates the amalgated script:

```shell
poetry run -- nox -s lua:amalgate
```

## Running Tests

AAF comes with different automated tests implemented in different programming languages and requiring different environments:

| Language | Category                        | Database | Environment |
|----------|---------------------------------|----------|-------------|
| Python   | Unit tests                      | no       | poetry      |
| Python   | Integration tests with database | yes      | _dev_env_   |
| Python   | Integration tests w/o database  | no       | _dev_env_   |
| Lua      | Unit tests                      | no       | _dev_env_   |

### Development Environment

For tests marked with Environment _dev_env_ you need to
* Setup a Development Environment
* Add the AAF to it
* Run the tests in the Development Environment

The Development Environment
* Activates AAF's conda environment containing a specific versino of Lua
* Sets the environment variables `LUA_PATH`, `LUA_CPATH`, and `PATH` for executing lua scripts

The following commands install the Development Environment and add the AAF
```shell
poetry run -- nox -s devenv:install
poetry run -- nox -s devenv:run -- poetry install
```

### Python Unit Tests

You can execute the unit tests without special preparation in the regular poetry environment:

```shell
poetry run -- pytest test/unit
```

### Python Integration Tests with and w/o database

The following commands run integration tests w/o and with database
```shell
poetry run -- nox -s devenv:pytest -- -- test/integration/no_db/
poetry run -- nox -s devenv:pytest -- -- --backend=onprem test/integration/with_db/
```

### Lua Unit Tests

The following command executes the Lua Unit Tests:
```shell
poety run nox -s lua:unit-tests
```
