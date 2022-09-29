
# User Guide

The goal of this library is to provide a general framework to implement complex 
data analysis algorithms with Exasol. This framework provides certain features 
to users in which they are able to run their implementations.

## Table of Contents

- [Getting Started](#getting-started)
- [Setup](#setup)
- [Usage](#usage)
- [Implementation of Algorithms](#implementation-of-algorithms)



## Getting Started
- Exasol DB
  - The Exasol cluster must already be running with version 7.1 or later.
  - DB connection information and credentials are needed.
- TODO: Connection
- TODO: Algorithm implementation


## Setup
### The Python Package
#### Download The Python Wheel Package
- The latest version of the python package of the framework can be 
downloaded from the Releases in GitHub Repository 
(see [the latest release](https://github.com/exasol/advanced-analytics-framework/releases/latest)).
Please download the following built archive:
```buildoutcfg 
advanced_analytics_framework.whl
```

#### Install The Python Wheel Package
- Install the packaged advanced-analytics-framework project as follows:
```bash
pip install exasol_advanced_analytics_framework.whl
```

### The Pre-built Language Container
#### Download Language Container
- In order to get this framework run, the language container of this framework is required.
- Please download the language container from the Releases in GitHub Repository 
(see [the latest release](https://github.com/exasol/advanced-analytics-framework/releases/latest)).


#### Install Language Container
- To install the language container, it is necessary to load the container into the BucketFS 
and register it to the database. The following command provides this setup:
```buildoutcfg
python -m exasol_advanced_analytics_framework.deploy language-container
    --dsn <DB_HOST:DB_PORT> \
    --db-user <DB_USER> \
    --db-pass <DB_PASSWORD> \
    --bucketfs-name <BUCKETFS_NAME> \
    --bucketfs-host <BUCKETFS_HOST> \
    --bucketfs-port <BUCKETFS_PORT> \
    --bucketfs-user <BUCKETFS_USER> \
    --bucketfs-password <BUCKETFS_PASSWORD> \
    --bucket <BUCKETFS_NAME> \
    --path-in-bucket <PATH_IN_BUCKET> \
    --language-alias <LANGUAGE_ALIAS> \ 
    --container-file <path/to/language_container.tar.gz>       
```

### Deployment
- Deploy all necessary scripts installed in the previous step to the specified 
`SCHEMA` in Exasol DB with the same `LANGUAGE_ALIAS`  using the following python cli command:
```buildoutcfg
python -m exasol_advanced_analytics_framework.deploy scripts
    --dsn <DB_HOST:DB_PORT> \
    --db-user <DB_USER> \
    --db-pass <DB_PASSWORD> \
    --schema <SCHEMA> \
    --language-alias <LANGUAGE_ALIAS>
```

## Usage
The entry point of this framework is `AAF_RUN_QUERY_HANDLER` script. This script is simply 
a Query Loop which is responsible for executing the implemented algorithm.

This script takes the necessary parameters to execute the desired algorithm in 
string json format. The json input includes two main part: 
  - `query_handler` : Details of the algorithm implemented by user.
  - `temporary_output`:  Information about BucketFS where the temporary outputs 
  of the query handler is kept.

You can find an example usage below:
```sql
EXECUTE SCRIPT AAF_RUN_QUERY_HANDLER('{
    "query_handler": {
        "factory_class": {
            "name": <CLASS_NAME>,
            "module": <CLASS_MODULE>
        },
        "parameters": <CLASS_PARAMETERS>
        "udf": {
            "name": <UDF_NAME>
            "schema": <UDF_SCHEMA_NAME>,
        },
    },
    "temporary_output": {
        "bucketfs_location": {
            "connection_name": <BUCKETFS_CONNECTION_NAME>
            "directory": <BUCKETFS_DIRECTORY>,
        },
        "schema_name": <SCHEMA_NAME>
    }
}')
```
Parameters 
 
 - `CLASS_NAME` : Name of the query handler class
 - `CLASS_MODULE`: Module name of the query handler class
 - `CLASS_PARAMETERS:` Parameters of the query handler class
 - `UDF_NAME` : Name of Python UDF script including user-implemented algorithm.
 - `UDF_SCHEMA_NAME`: Schema name where the UDF script is deployed.
 - `BUCKETFS_CONNECTION_NAME`: BucketFS connection name to keep temporary outputs
 - `BUCKETFS_DIRECTORY`: Directory in BucketFS where temporary outputs are kept


# Implementation of Algorithms

The algorithm should extend the `UDFQueryHandler` abstract class  and then 
implement its following methods:

- `start()` : It is called at the first execution of the framework, that is, 
in the first iteration. It returns either _Continue_ or _Finish_ result objects. 
While _Finish_ result object returns the final result of the implemented algorith, 
_Continue_ object returns the query list that will be executed for the next state.
- `handle_query_result()` : This method is get called at the following iterations 
to handle the return query. An example class definition is given below:

```python
class CustomQueryHandler(UDFQueryHandler):

    def __init__(self, parameter: str, query_handler_context: QueryHandlerContext):
        super().__init__(parameter, query_handler_context)

    def start(self) -> Union[Continue, Finish[ResultType]]:
        pass

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[str]]:
        pass
```
