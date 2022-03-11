# Design


## Introduction
This library is an event-driven framework that allows users to build complex 
data processing algorithms in Exasol.

## Constraints / Limitations

- The most efficient way to run SQL queries is to use Lua script. Because Lua 
scripting enable us to run queries in the same transaction using `pquery` method.
- The results of `pquery` are simple handles and there is a limit of 250 open 
result set handles for Lua scripts.


## Assumptions

- Since the number open result set handles in Lua scripts is limited, it is 
assumed that Lua scripts is not nested more than 250 times.
- The result of pquery is assumed to not exceed the db memory limit in size. 
The DB usually allows 4 GB per query and node. The UDF can consume up to 32 GB 
per node, but get throttle down after the 4 GB. However, the 32 GB are shared 
between all queries on a node.
- The state of the algorithm is not too large.
- The (de-)serialization and the upload/download of the state to the BucketFS 
take less than 3 seconds.



## Design Considerations

- The main logic of the framework should be implemented in Python, which is 
widely used in data processing and analysis.

- The two main components of the framework, which are an Event Loop and an Event 
Handler, should be decoupled so that they can be independently updated and 
implemented. It also allows us to replace them with a different implementation 
in another programming language.

- The Event Handler should be able to keep track of the status of a particular 
operation by storing states in BucketFS.


## System Design and Architecture
 
TBD 



## Event Loop
Event Loop  processes only the state transitions by executing queries returned 
by the Event Handler. This loop provides the new state transition by calling 
the Event Handler until the Event Handler returns the stop signal with a result 
or an error.

### Initiating Event Loop
`dsn~initiating-event-loop~1`

The Event Loop is responsible for getting the parameters of the Event Handler 
that will be called in the loop and forward them to it.

Covers:

* `req~intiating-global-loop~1`

Needs: impl, utest, itest

### Handling States
`dsn~handling-states~1`

The  output of the called Event Handler query can include SQL queries to 
complete its computation. In that case Event Loop run them on each iteration, 
until the Event Handler returns the status as complete.

Covers:

* `req~iterating-over-loop~1`

Needs: impl, utest, itest


### Generating Return Query
`dsn~generating-return-query~1`

At each iteration the return query of the Event Handler is rebuild with current 
state information and called again as the next state action so that  the new 
state transition is performed.

Covers:

* `req~generating-return-query~1`

Needs: impl, utest, itest


### Returning JSON Result
`dsn~returning-json-result~1`

The Event Loop returns result in json format.

Covers:

* `req~returning-result~1`

Needs: impl, utest, itest



## Event Handler
The Event Handler is a framework whose logic is implemented by users. It reacts 
to the query results of  the Event Loop and generates SQL queries of the next 
state. It stores the states in BucketFS.


### Python UDF Framework 
`dsn~python-udf-framework~1`

An Event Handler designed as Python UDF  allows users to have a easy-to-use and 
better tooling framework to build their algorithm on top it.

Covers:

* `req~user-friendly-framework-interface~1`

Needs: impl, utest, itest


### Storing States in BucketFS 
`dsn~storing-states-in-bucketfs~1`

The framework  keeps states during iterations by storing them in BucketFS.

Covers:

* `req~handling-states~1`

Needs: impl, utest, itest


### Managing Temporary BucketFS Files
`dsn~managing-temporary-bucketfs-files~1`

The Event Handler create temporary BucketFS files that will be  kept as result. 
These temporary files are placed  in the same directory in  the BucketFS.   

Covers:

* `req~managing-temporary-bucketfs-files~1`

Needs: impl, utest, itest


### Managing Temporary BucketFS Files
`dsn~managing-temporary-tables~1`

The Event Handler uses temporary tables to store large intermediate results. 

Covers:

* `req~managing-temporary-tables~1`

Needs: impl, utest, itest


### Error Handling
In case of any errors in the execution, temporarily created files and tables 
are cleaned. 


#### Cleanup Temporary Tables
`dsn~cleanup-temporary-tables~1`

Temporary tables are removed in case of an error that occur during the execution 
of the framework with the keeping option  not selected.

Covers:

* `req~cleanup-temporary-tables~1`

Needs: impl, utest, itestn



#### Cleanup Temporary BucketFS Files
`dsn~cleanup-temporary-bucketfs-files~1`

Temporary BucketFS files are removed in case of an error that occur during 
the execution of the framework with the keeping option  not selected.

Covers:

* `req~cleanup-temporary-bucketfs-files~1`

Needs: impl, utest, itest
