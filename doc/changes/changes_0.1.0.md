# advanced-analytics-framework 0.1.0, released XXXX-XX-XX


## Summary


### Features

  - #1: Added the initial setup of the project
  - #4: Added the design document
  - #5: Added the system requirements document
  - #7: Added Lua event loop
  - #6: Added Python event handler
  - #24: Added integrations test for event loop
  - #28: Extended the EventHandlerContext to a scope-based system for handling temporary objects
  - #29: Fixed interface of EventContext and adapted implementation of UDFEventContext
  - #30: Sort cleanup queries in reverse order of their creation to ensure that temporary objects that depend on other are removed first
  - #34: Added MockEventContext
  - #35: Clean up after EventHandler exceptions and throw exceptions when a parent EventHandlerContext encounters an unreleased child during release 

### Bug Fixes

  - #8: Renamed master branch to main

### Refactoring

 - #42: Updated dependencies
  
### Documentation

  - #9: Added README file
  