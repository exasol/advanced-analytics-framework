# Design


## Introduction
The goal of this library is to provide a general framework to implement complex 
data analysis algorithms with Exasol. The framework basically runs the necessary 
SQL queries for a given operation in a loop until the state of the operations 
is completed.

## Constraints / Limitations

- The most efficient way to run SQL queries is to use Lua script.  Only 
available in Lua Scrips, `pquery` allows SQL queries to be run in the same 
transactions.

- The results of `pquery` are simple handles and there is a limit of 250 open 
result set handles for Lua scripts.


## Assumptions

- Since the number open result set handles in Lua scripts is limited, it is 
assumed that Lua scripts is not nested more than 250 times.

- The result of pquery is assumed to not exceed the limit in size.

## Design Considerations

- The main logic of the framework should be implemented in Python, which is 
widely used in data processing and analysis.

- The two main components of the framework, which are Even Loop and Even Handler, 
should be decoupled so that they can be independently updated and implemented.

- The Event Handler should be able to keep track of the status of a particular 
operation by storing states in BucketFS.


## System Design and Architecture
 
TBD 



## Event Loop
`dsn~event-loop~1`
Event Loop  processes only the state transitions by executing queries returned 
by the Event Handler. This loop provides the new state transition by calling 
the Event Handler until the state is complete. Furthermore, the Event Loop 
is responsible for getting the parameters of the Event Handler that will be 
called in the loop and forward them to it.

Covers:

* `feat~TBD~1`

Needs: impl, utest, itest


## Event Handler
The Event Handler is a framework whose logic is implemented by users. It reacts 
to the query results of  the Event Loop and generates SQL queries of the next 
state. It stores the states in BucketFS.

Covers:

* `feat~TBD~1`

Needs: impl, utest, itest