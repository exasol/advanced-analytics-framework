# Developer Guide

The developer guide explains how to maintain and develop the Advanced Analytics Framework (AAF).

* [developer_environment](developer_environment.md)

## Building and Installing the AAF Script Language Container (SLC)

The following command builds the SLC for the AAF

```shell
poetry run nox -s build_language_container
```

Installing the SLC ins described in the [AAF User Guide](../user_guide/user_guide.md#script-language-container-slc).

## Updated Generated Files

AAF contains some generated files that are committed to git, though:
* The amalgated Lua script [create_query_loop.sql](https://github.com/exasol/advanced-analytics-framework/blob/main/exasol_advanced_analytics_framework/resources/outputs/create_query_loop.sql)
* The examples in the user guide

The amalgated Lua script originates from the files in directory [exasol_advanced_analytics_framework/lua/src](https://github.com/exasol/advanced-analytics-framework/blob/main/exasol_advanced_analytics_framework/lua/src/).

The following command updates the amalgated script:

```shell
poetry run nox -s amalgate_lua_scripts
```

AAF's user guide contains an example for an adhoc implementation of a Query Handler originating from the files in directory [exasol_advanced_analytics_framework/example](https://github.com/exasol/advanced-analytics-framework/blob/main/exasol_advanced_analytics_framework/example/).

The following command updates the example in the uiser guide:

```shell
poetry run nox -s update_user_guide
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
poetry run -- nox -s install_dev_env
poetry run -- nox -s run_in_dev_env -- poetry install
```

### Python Unit Tests

You can execute the unit tests without special preparation in the regular poetry environment:

```shell
poetry run pytest tests/unit_tests
```

### Python Integration Tests with and w/o database

The following commands run integration tests w/o and with database
```shell
poetry run -- nox -s run_python_test -- -- tests/integration_tests/without_db/
poetry run -- nox -s run_python_test -- -- --backend=onprem tests/integration_tests/with_db/
```

### Lua Unit Tests

The following command executes the Lua Unit Tests:
```shell
poety run nox -s run_lua_unit_tests
```
