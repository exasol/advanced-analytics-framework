# Design


## Introduction
This document describes the design of the advanced analytics framework on which 
complex data analysis algorithms can be built.

## Constraints / Limitations

- The best way to run dynamic SQL queries is to use Lua scripts. Because Lua
scripting enables us to run queries in the same transaction using the `pquery` method.
- The results of `pquery` are simple handles and there is a limit of 250 open 
result set handles for Lua scripts.
- The main logic of the framework should be implemented in Python, because it is 
widely used in data processing and analysis.
- Python UDF, where the main logic is placed, returns the result of the query 
and finishes its execution. Due to that, it cannot be kept running and used 
as server.


## Assumptions

- Since the number open result set handles in Lua scripts is limited, it is 
assumed that Lua scripts is not nested more than 250 times.
- The result of `pquery` is assumed to not exceed the db memory limit in size. 
The DB usually allows 4 GB per query and node. The UDF can consume up to 32 GB 
per node, but gets throttled down after the 4 GB. However, the 32 GB are shared 
between all queries on a node.
- The state of the algorithm is not too large.
- The (de-)serialization and the upload/download of the state to the BucketFS 
take less than 3 seconds.



## Design Considerations

Due to the fact that pquery is only available for Lua and Python is useful to 
implement the user code, we proposed an event-driven design for this framework. 
The designed framework is divided into two parts:

- The first part needs to be a Lua Script which is responsible for running SQL queries.
- The second part runs the python user code and is responsible for generating the SQL queries

Because Python UDF cannot be kept running and used as server, we have to call for each iteration the UDF again.
- This means the UDF has to store and load its state each time it is getting called. 
- Furthermore, the interface to the user code needs to be suitable for this form of execution. 
- The user code can't wait actively for the result of a query. The framework will execute it while the UDF is not running anymore.
    - The user code has to get called again, when the SQL queries are ready. 
    - The user code is a kind of callback, however a call back with state.
    - To this type of execution, the model of an event handler fits best.
    - This means the Lua Script is our event loop.

## System Design and Architecture
 
The designed event-driven framework consists of 3 
main components: (1) Even Loop that handles only the state transitions, (2) 
Event Handler Framework that defines a state machine and provide a framework to 
the user code, (3) Event Handler, where the user implements their algorithm. The Event Loop 
component is proposed to be implemented in Lua, because a Lua script is the only 
way to run the dynamic SQL queries in the same transactions. On the other hand, 
the Event Handler is implemented in Python script, since Python simplifies the 
development of data analysis methods by offering a wide variety of data 
processing tools.


## Event Loop
The Event Loop processes only the state transitions by executing queries returned 
by the Event Handler. This loop provides the new state transition by calling 
the Event Handler until the Event Handler returns the stop signal with a result 
or an error.

### Initiating Event Loop
`dsn~initiating-event-loop~1`

The Event Loop is responsible for getting the parameters of the Event Handler 
that will be called in the loop and forward them to it.

Covers:

* `req~intiating-algorithm~1`

Needs: impl, utest, itest

### Iterating over Loop
`dsn~handling-states~1`

The output of the called Event Handler query can include SQL queries to 
complete its computation. In that case the Event Loop runs them on each iteration, 
until the Event Handler returns the status as complete.

Covers:

* `req~iterating-over-loop~1`

Needs: impl, utest, itest



### Returning JSON Result
`dsn~returning-json-result~1`

The Event Loop returns its result in json format.

Covers:

* `req~returning-result~1`

Needs: impl, utest, itest



## Event Handler
The Event Handler is a framework whose logic is implemented by the users. It reacts 
to the query results of the Event Loop and generates SQL queries of the next 
state. It stores the states in BucketFS.


### Python UDF Framework 
`dsn~python-udf-framework~1`

An Event Handler designed as Python UDF instead of a Lua script allows users to have an easy-to-use and 
better tooling framework to build their algorithm on top of. The user can develop 
the algorithm as a Python UDF script in which the framework is imported. Some 
details of this framework interface are listed below:

- The event handler is implemented as a class and states are kept as attributes 
of a class to facilitate the states operations.
- The user code should implement a method `handle_event` where the logic of the 
code is placed.
- The event handler is initiated by getting the following inputs: The results of 
the return query, the next state information including its parameters, 
the ExasolDB context object. 
- Upon completion of the algorithm, the Event Handler calls methods itself to 
remove temporary records.
- In case of an error caught in the Event Loop, If the keep option is not set, 
the cleanup event is called for the Event Handler and the temporary records are 
deleted. Otherwise, they are kept and can be used for further investigations such as debugging.

Covers:

* `req~user-friendly-framework-interface~1`

Needs: impl, utest, itest


### Storing States in BucketFS 
`dsn~storing-states-in-bucketfs~1`

The framework keeps states during iterations by storing them in BucketFS.

Covers:

* `req~managing-temporary-bucketfs-files~1`

Needs: impl, utest, itest


It returns the sequence of SQL queries to execute
a query to call the same UDF again to run the action for the next state.

### Returning Queries
`dsn~returning-queries~1`

The Event Handler returns a list of SQL queries to execute and the return query 
which will be called again as the next state action so that the new  state 
transition is performed.

Covers:

* `feat~implementation-framework~1`

Needs: impl, utest, itest

### Managing Temporary BucketFS Files
`dsn~managing-temporary-bucketfs-files~1`

The Event Handler create temporary BucketFS files that will be  kept as result. 
These temporary files are placed in the same directory in the BucketFS.   

Covers:

* `req~managing-temporary-bucketfs-files~1`

Needs: impl, utest, itest


### Managing Temporary BucketFS Files
`dsn~managing-temporary-tables~1`

The Event Handler uses temporary tables to store large intermediate results. 

Covers:

* `req~managing-temporary-tables~1`

Needs: impl, utest, itest


### Electing Leader
`dsn~electing-leader~1`

The leader selection consist of 3 main steps: Firstly, each UDF is assigned a 
unique id which is the concatenation of node_id and vm_id. Secondly, the list of 
UDF instances are discovered using pycos. Thirdly,  the UDF instance with the 
largest id is elected as the leader. Other instances send a confirmation message 
to the  instance with the highest id that it is the leader. If the leader got 
all the confirmation, it acknowledges them. 

Covers:

* `req~electing-leader~1`

Needs: impl, utest, itest


### Collective Operation
`dsn~collective-operation~1`

The framework uses a collective operation approach, where tasks are simultaneously 
run on multiple UDF instances to achieve parallelism.

Covers:

* `req~collective-operation~1`

Needs: impl, utest, itest


### Error Handling
In case of any errors in the execution, temporarily created files and tables 
are cleaned. 


#### Cleanup Temporary Tables
`dsn~cleanup-temporary-tables~1`

Temporary tables are removed in case of an error occuring during the execution 
of the framework with the keeping option not selected.

Covers:

* `req~cleanup-temporary-tables~1`

Needs: impl, utest, itest



#### Cleanup Temporary BucketFS Files
`dsn~cleanup-temporary-bucketfs-files~1`

Temporary BucketFS files are removed in case of an error that occur during 
the execution of the framework with the keeping option  not selected.

Covers:

* `req~cleanup-temporary-bucketfs-files~1`

Needs: impl, utest, itest
