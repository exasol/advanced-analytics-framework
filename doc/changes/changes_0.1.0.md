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
* #30: Sort cleanup queries in reverse order of their creation to ensure that temporary objects that depend on other are removed first
* #34: Added MockEventContext
* #35: Clean up after EventHandler exceptions and throw exceptions when a parent EventHandlerContext encounters an unreleased child during release
* #94: Prepare for release to PyPi
* #17: Added vagrant setup
* #97: Added SocketFactory wrapper which injects faults by losing messages
* #98: Added more robust connection protocol
* #99: Added multi/node udf discovery
* #100: Add combined global and local UDF discovery

### Bug Fixes

* #8: Renamed master branch to main
* #62: Fixed bug in ScopeQueryHandlerContext transfer_object_to
* #64: Removed `_check_if_released` calls in `__eq__` and `__hash__` for DBObjectNameProxy
* #65: Fixed that the `ScopeQueryHandlerContext` might not `_release` all child contexts, if a grand-child-context wasn't released
* #68: Fixed that methods called in BucketFSLocationProxy.cleanup can fail and stop the cleanup
* #66: Fixed _ScopeQueryHandlerContextBase.get_temporary_path not being private
* #116: Fix AbortTimeoutSender and add reason to Timeout messages

### Refactoring

* #171: Updated poetry dependencies
* #42: Updated dependencies
* #72: Unified naming of released resources in QueryHandler
* #88: Introduced an abstraction for ZMQ in UDF Communication
* #95: Remove setup.py
* #114: Refactored BackgroundPeerState and introduced parameter objects
* #173: Introduced Python Toolbox
* #174: Replaced Language Container Stuff with PEC
* #183 Fixed warning on tests with `__init__` constructor

### Documentation

* #9: Added README file
