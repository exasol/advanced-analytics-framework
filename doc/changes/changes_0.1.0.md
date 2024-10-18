# advanced-analytics-framework 0.1.0, released TBD

Code name:

## Summary


### Features

* #1: Added the initial setup of the project
* #4: Added the design document
* #5: Added the system requirements document
* #7: Added Lua event loop
* #6: Added Python event handler
* #24: Added integrations test for event loop
* #28: Extended the EventHandlerContext to a scope-based system for handling temporary objects
* #29: Fixed interface of EventContext and adapted implementation of UDFEventContext
* #30: Sorted cleanup queries in reverse order of their creation to ensure that temporary objects that depend on other are removed first
* #34: Added MockEventContext
* #35: Cleaned up after EventHandler exceptions and throw exceptions when a parent EventHandlerContext encounters an unreleased child during release
* #94: Prepare for release to PyPi
* #17: Added vagrant setup
* #97: Added SocketFactory wrapper which injects faults by losing messages
* #98: Added more robust connection protocol
* #99: Added multi/node udf discovery
* #100: Added combined global and local UDF discovery
* #177: Added proxies for UDFs and Connections

### Bug Fixes

* #8: Renamed master branch to main
* #62: Fixed bug in ScopeQueryHandlerContext transfer_object_to
* #64: Removed `_check_if_released` calls in `__eq__` and `__hash__` for DBObjectNameProxy
* #65: Fixed that the `ScopeQueryHandlerContext` might not `_release` all child contexts, if a grand-child-context wasn't released
* #68: Fixed that methods called in BucketFSLocationProxy.cleanup can fail and stop the cleanup
* #66: Fixed _ScopeQueryHandlerContextBase.get_temporary_path not being private
* #116: Fixed AbortTimeoutSender and add reason to Timeout messages

### Refactoring

* #171: Updated poetry dependencies
* #42: Updated dependencies
* #72: Unified naming of released resources in QueryHandler
* #88: Introduced an abstraction for ZMQ in UDF Communication
* #95: Removed setup.py
* #114: Refactored BackgroundPeerState and introduced parameter objects
* #173: Introduced Python Toolbox
* #174: Replaced Language Container Stuff with PEC and SLC plugin
* #183: Fixed warning on tests with `__init__` constructor
* #180: Replaced `start_integration_test_environment.sh` with `pytest-backend-plugin`
* #184: Updated micromamba to the latest version 2.0.0
* #176: Updated usage of `exasol-bucketfs` to new API
* #185: Removed directory and script for building SLC AAF
* #191: Renamed UDF json element "parameters" to "parameter"
* #178: Fixed names of mock objects:
  * Renamed `testing.mock_query_handler_runner.MockQueryHandlerRunner` to `query_handler.python_query_handler_runner.PythonQueryHandlerRunner`
  * Renamed method `PythonQueryHandlerRunner.execute_query()` to `execute_queries()`
  * Renamed `mock_query_result.MockQueryResult` to `python_query_result.PythonQueryResult`

### Documentation

* #9: Added README file
* #26: Added user guide

## Dependency Updates

Compared to `main` branch this release updates the following dependencies:

### File `pyproject.toml`

* Updated dependency `exasol-bucketfs:0.8.0` to `0.13.0`
* Updated dependency `pyexasol:0.25.2` to `0.27.0`
* Updated dependency `typeguard:2.13.3` to `4.3.0`
* Updated dependency `exasol-integration-test-docker-environment:3.1.0` to `3.2.0`
* Updated dependency `polyfactory:2.16.2` to `2.17.0`
* Added dependency `exasol-python-extension-common:0.6.0`
* Added dependency `exasol-script-languages-container-tool:1.0.0`
* Added dependency `pytest-exasol-slc:0.3.0`
* Added dependency `pytest-exasol-backend:0.3.0`
* Added dependency `pytest-exasol-extension:0.1.0`
