# Developer Guide

The developer guide explains how to maintain and develop the Advanced Analytics Framework (AAF).

* [developer_environment](developer_environment.md)

## Building and Installing the AAF Script Language Container (SLC)

The following command builds the SLC for the AAF

```shell
poetry run nox -s build_language_container
```

Installing the SLC ins described in the [AAF User Guide](../user_guide/user_guide.md#script-language-container-slc).

## Running Tests

AAF comes with different automated tests implemented in different programming languages and requiring different environments:

| Language | Category                        | Database | Environment |
|----------|---------------------------------|----------|-------------|
| Python   | Unit tests                      | no       | poetry      |
| Python   | Integration tests with database | yes      | _dev_env_   |
| Python   | Integration tests w/o database  | no       | _dev_env_   |
| Lua      | Unit tests                      | no       | _dev_env_   |

### The Special _Development Environment_

For tests marked with Environment _dev_env_ you need to
* Install the LUA environment
* Install the AAF into the _Development Environment_
* Run the tests in the _Development Environment_

The Development Environment
* Activates AAF's conda environment <!-- Why is this required? What does it do in particular? -->
* Sets the environment variables `LUA_PATH`, `LUA_CPATH`, and `PATH` for executing lua scripts

The following commands installs the LUA environment and the AAF within the _Development Environment_:
```shell
poetry run -- nox -s install_lua_environment
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
