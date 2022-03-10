# System Requirement Specification Advanced Analytics Framework

## Introduction

This library is an event-driven framework that allows users to build complex 
data processing algorithms in Exasol.


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

### Event Loop
`feat~event-loop~1`

Event Loop processes only the state transitions by executing queries returned by 
the Event Handler. Some complex algorithms need a global loop. In each iteration 
of the loop they need to execute SQL queries until the algorithm no longer 
needs to run further queries. 

Needs: req

### Event Handler
`feat~event-handler~1`

TODO

Needs: req


## Functional Requirements

This section lists functional requirements. The requirements are grouped by 
feature where they belong to a single feature.

### Event Loop

#### Calling Event-Handler
`req~calling-event-handler~1`

It takes the configuration of an operation, which is implemented by user, as 
input. It builds query using this configuration to call Event Handler. 
Furthermore, in each iteration the return query of the Event Handler is called 
again as the next state action and performed the new state transition. 

Covers:

* [feat~event-loop~1](#event-loop)

Needs: dsn


#### Running Queries
`req~running-queries~1`

The output of the called Event Handler operation contains the necessary SQLs 
if the algorithm needs to continue. This function runs these required SQLs.

Covers:

* [feat~event-loop~1](#event-loop)

Needs: dsn


#### Generating Return Query
`req~generating-return-query~1`

When the operation needs to continue, the Event Handler returns the next query 
of the Event-Handler that should get called again.  This method builds the 
return query using the current state information and make it ready to be 
called as a new state.

Covers:

* [feat~event-loop~1](#event-loop)

Needs: dsn


### Event Handler

#### Handling States
`req~TODO~1`

Covers:

* [feat~event-handler~1](#event-handler)

Needs: dsn


#### Managing Storage
`req~TODO~1`

Covers:

* [feat~event-handler~1](#event-handler)

Needs: dsn


#### Managing Temporary Tables
`req~TODO~1`

Covers:

* [feat~event-handler~1](#event-handler)

Needs: dsn
