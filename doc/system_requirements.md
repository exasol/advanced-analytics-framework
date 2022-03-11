# System Requirement Specification Advanced Analytics Framework

## Introduction

This document describes the system requirements for the advanced analytics 
framework on which complex algorithms can be built.


## About This Document

### Goal

The goal of this library is to provide a general framework to implement complex 
data analysis algorithms with Exasol. This framework provides certain features 
to users in which they are able to run their implementations.

## Stakeholders

Software developers use this framework to develop complex data processing 
algorithms such as machine learning methods.

## Features

Features are the highest level requirements in this document that describe the 
main functionality of Advanced Analytics Framework.

### Global Loop
`feat~global-loop~1`

Some complex algorithms need a global loop. In each iteration of the loop, they 
need to execute SQL queries until the algorithm no longer needs to run 
further queries. 

Needs: req

### Implementation Framework
`feat~implementation-framework~1`

The logic of the algorithm is implemented by user. The implementation needs 
to call a framework which provides certain features to the user code, such as 
automatic state handling. 

Needs: req


### Error Handling
`feat~error-handling~1`

Any errors, that may occur during state transitions or  the execution of queries, 
should be handled. Temporarily created files and tables need to be cleaned.
User can set option to keep temporary records. It can be useful to keep these 
for debugging.

Needs: req


## Functional Requirements

This section lists functional requirements. The requirements are grouped by 
feature where they belong to a single feature.



### Global Loop

#### Initiating Global Loop
`req~intiating-global-loop~1`

It expects the configuration of the operation, which is implemented by user, as 
input. It needs to build starting query using this configuration. 

Covers:

* [feat~global-loop~1](#global-loop)

Needs: dsn

#### Iterating over Loop
`req~iterating-over-loop~1`

At each iteration, the framework checks the output of the called operation query 
to decide whether to continue. If the output contains the necessary SQLs and the 
status information indicating that it is in progress, these SQLs should run as 
the next state action and perform the new state transition. 

Covers:

* [feat~global-loop~1](#global-loop)

Needs: dsn


#### Generating Return Query
`req~generating-return-query~1`

When the operation needs to continue, the output of the operation query returns
the next query that should get called again.  It is required to build the  
return query using the current state information and make it ready to be  
called as a new state.

Covers:

* [feat~global-loop~1](#global-loop)

Needs: dsn

#### Returning Result
`req~returning-result~1`

The algorithm should return a value in json format.

Covers:

* [feat~global-loop~1](#global-loop)

Needs: dsn




### Implementation Framework

#### User-Friendly Framework Interface
`req~user-friendly-framework-interface~1`

The algorithm is implemented by the user, thus the interface needs to be simple 
for everyone to implement algorithms. Furthermore, it is required to implement
it in Python, because Python has a larger community and provides better tooling 
for complex data analysis.

Covers:

* [feat~implementation-framework~1](#implementation-framework)

Needs: dsn

#### Handling States
`req~handling-states~1`

The framework should keep track of states during iterations. To do this, it 
needs to access BucketFS and store its state there.

Covers:

* [feat~implementation-framework~1](#implementation-framework)

Needs: dsn


#### Managing Temporary BucketFS Files
`req~managing-temporary-bucketfs-files~1`

The algorithm should be able to create temporary BucketFS files that will be 
kept as result. These temporary file should be placed  in the same directory in 
the BucketFS so that it will be easy to access them.

Covers:

* [feat~implementation-framework~1](#implementation-framework)

Needs: dsn


#### Managing Temporary Tables
`req~managing-temporary-tables~1`

The framework uses temporary tables to store large intermediate results. The 
system should make sure that these are cleaned up.


Covers:

* [feat~implementation-framework~1](#implementation-framework)

Needs: dsn



### Error Handling

#### Cleanup Temporary Tables
`req~cleanup-temporary-tables~1`

Temporary tables should be removed in case of an error that occur during the 
execution of the framework with the keeping option  not selected.

Covers:

* [feat~error-handling~1](#error-handling)

Needs: dsn


#### Cleanup Temporary BucketFS Files
`req~cleanup-temporary-bucketfs-files~1`

Temporary BucketFS files should be removed in case of an error that occur during 
the execution of the framework with the keeping option  not selected.

Covers:

* [feat~error-handling~1](#error-handling)

Needs: dsn




