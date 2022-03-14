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
temporary table handling, running the queries and returning the results. 
Furthermore, the framework should handle message passing in parallel computing. 

Needs: req


### Error Handling
`feat~error-handling~1`

Any errors, that may occur during user code execution or the execution of queries, 
should be handled. Temporarily created files and tables need to be cleaned.
User can set option to keep temporary records. It can be useful to keep these 
for debugging.

Needs: req


## Functional Requirements

This section lists functional requirements. The requirements are grouped by 
feature where they belong to a single feature.



### Global Loop

#### Initiating Algorithm
`req~intiating-algorithm~1`

The algorithm expects a configuration of the operation, which is implemented 
by user, as input. 

Covers:

* [feat~global-loop~1](#global-loop)

Needs: dsn

#### Iterating over Loop
`req~iterating-over-loop~1`

At each iteration, the framework asks the user code to check the output of the 
executed query to decide whether to continue. If the user code decides to stop 
the framework returns the results returned by the user code. Otherwise, the user 
code can start another iteration with further queries.

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

#### Managing Temporary BucketFS Files
`req~managing-temporary-bucketfs-files~1`

The algorithm should be able to access BucketFS and create temporary BucketFS 
files that will be kept as result. These temporary file should be placed  in the 
same directory in the BucketFS so that it will be easy to access them.

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


#### Message Passing in Distributed Computation
`req~message-passing-in-distributed-computation~1`

The data used by the algorithm might be distributed to the UDF instances. The 
framework should enable message passing between these UDF instances to access 
data or intermediate results of the other partitions.

Covers:

* [feat~implementation-framework~1](#implementation-framework)

Needs: dsn


#### Electing Leader
`req~electing-leader~1`

The framework needs to elect a leader from among the UDF instances.

Covers:

* [feat~implementation-framework~1](#implementation-framework)

Needs: dsn


#### Achieving Parallelism
`req~collective-operation~1`

The framework must support Collective operation on UDF instances to achieve 
parallelism.

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

