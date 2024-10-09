# Advanced Analytics Framework User Guide

The Advanced Analytics Framework (AAF) enables implementing complex data analysis algorithms with Exasol. Users can use the features of AAF in their custom implementations.

## Table of Contents

* [Setup](#setup)
* [Usage](#usage)
* [Implementation of Custom Algorithms](#implementation-of-custom-algorithms)

## Setup

### Exasol database

* The Exasol cluster must already be running with version 7.1 or later.
* Database connection information and credentials are needed for the database itself as also for the BucketFS.

### BucketFS Connection

AAF employs some Lua scripts and User Defined Functions (UDFs).  The Lua
scripts are orchestrating the UDFs while the UDFs are performing the actual
analytic functions.

AAF keeps a common state of execution and passes input data and results
between Lua and UDFs via files in the Bucket File System (BucketFS) of the
Exasol database.

<!-- For keeping a common state of execution and passing input data and
results between Lua and UDFs AAF requires to access the Bucket File System
(BucketFS) of the Exasol database. -->

The following SQL statements create such a connection to the BucketFS:

```sql
CREATE OR REPLACE CONNECTION '<CONNECTION_NAME>'
TO '{
  "backend": "<BACKEND>",
  "url": "<HOST>:<PORT>",
  "service_name": "<SERVICE_NAME>",
  "bucket_name": "<BUCKET_NAME>",
  "path": "<PATH>",
  "verify": <VERIFY>,
  "host": "<SAAS_HOST>",
  "account_id": "<SAAS_ACCOUNT_ID>",
  "database_id": "<SAAS_DATABASE_ID>",
  "pat": "<SAAS_PAT>"
  }'
USER '{"username": "<USER_NAME>"}'
IDENTIFIED BY '{"password": "<PASSWORD>"}' ;
```

The list of elements in the connection's parameter called `TO` depends on the
backend you want to use. There are two different backends: `onprem` and
`saas`.

The following table shows all elements for each of the backends.

| Backend  | Parameter            | Required? | Default value  | Description                                                        |
|----------|----------------------|-----------|----------------|--------------------------------------------------------------------|
| (any)    | `<CONNECTION_NAME>`  | yes       | -              | Name of the connection                                             |
| (any)    | `<USER_NAME>`        | -         | `true`         | Name of the user accessing the Bucket (requires  write permissions) |
| (any)    | `<PASSWORD>`         | -         | `true`         | Password for accessing the Bucket (requires  write permissions)    |
| (any)    | `<BACKEND>`          | yes       | -              | Which backend to use, must be either `onprem` or `saas`            |
| `onprem` | `<HOST>`             | yes       | -              | Fully qualified Hostname or ip Address                             |
| `onprem` | `<PORT>`             | -         | `2580`         | Port of the BucketFS Service                                       |
| `onprem` | `<SERVICE_NAME>`     | yes       | `bfsdefault`   | Name of the BucketFS Service                                       |
| `onprem` | `<BUCKET_NAME>`      | yes       | `default`      | Name of the Bucket                                                 |
| `onprem` | `<PATH>`             | -         | (empty / root) | Path inside the Bucket                                             |
| `onprem` | `<VERIFY>`           | -         | `true`         | Whether to apply TLS security to the connection                    |
| `saas`   | `<SAAS_ACCOUNT_ID>`  | yes       | -              | Account ID for accessing an SaaS database instance                 |
| `saas`   | `<SAAS_DATABASE_ID>` | yes       | -              | Database ID of an Exasol SaaS database instance                    |
| `saas`   | `<SAAS_PAT>`         | yes       | -              | Personal access token for accessing an SaaS database instance      |

### AAF Python Package

The latest version of AAF can be obtained from [pypi](https://pypi.org), see also the [releases on GitHub](https://github.com/exasol/advanced-analytics-framework/releases).

The following command installs the AAF from pypi:

```bash
pip install exasol-advanced-analytics-framework
```

### Script Language Containers (SLCs)

Exasol executes User Defined Functions (UDFs) in an isolated Script Language Container (SLCs).
Running the AAF requires a custom SLC.

#### Build the SLC

If you want to build the AAF SLC then

1. Download the [AAF Sources](https://github.com/exasol/advanced-analytics-framework/) from GitHub
2. Install poetry
3. Use the AAF developer commands for building and deploying custom SLCs into Exasol.

```shell
poetry run nox build_language_container
```

See [Install the SLC](#install-the-slc).

#### Download a Pre-built SLC

As an alternative to building SLC yourself you can also download a prebuilt AAF SLC from the [AAF releases](https://github.com/exasol/advanced-analytics-framework/releases/latest) on GitHub.

#### Install the SLC

Installing the SLC requires loading the container file into the BucketFS and registering it to the database:

```shell
SLC_FILE=.slc/exasol_advanced_analytics_framework_container_release.tar.gz
LANGUAGE_ALIAS=PYTHON3_AAF
python -m exasol_advanced_analytics_framework.deploy language-container \
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
    --language-alias "$LANGUAGE_ALIAS" \
    --container-file "$SLC_FILE"
```

### Additional Scripts

Besides the BucketFS connection, the SLC, and the Python package AAF also requires some additional Lua scripts to be created in the Exasol database.

The following command deploys the additional scripts to the specified `DB_SCHEMA` using the `LANGUAGE_ALIAS` of the SLC:

```shell
python -m exasol_advanced_analytics_framework.deploy scripts \
    --dsn <DB_HOST:DB_PORT> \
    --db-user <DB_USER> \
    --db-pass <DB_PASSWORD> \
    --schema <DB_SCHEMA> \
    --language-alias "$LANGUAGE_ALIAS"
```

## Usage

The entry point of this framework is `AAF_RUN_QUERY_HANDLER` script. This script is simply a query loop which is responsible for executing the implemented algorithm.

This script takes the necessary parameters to execute the desired algorithm in string json format. The json input includes two main part:

* `query_handler` : Details of the algorithm implemented by user.
* `temporary_output`:  Information about BucketFS where the temporary outputs of the query handler is kept.

The following SQL statement shows how to call an AAF query handler:

```sql
EXECUTE SCRIPT AAF_RUN_QUERY_HANDLER('{
    "query_handler": {
        "factory_class": {
            "module": "<CLASS_MODULE>",
            "name": "<CLASS_NAME>"
        },
        "parameters": "<CLASS_PARAMETERS>",
        "udf": {
            "schema": "<UDF_DB_SCHEMA>",
            "name": "<UDF_NAME>"
        }
    },
    "temporary_output": {
        "bucketfs_location": {
            "connection_name": "<BUCKETFS_CONNECTION_NAME>",
            "directory": "<BUCKETFS_DIRECTORY>"
        },
        "schema_name": "<TEMP_DB_SCHEMA>"
    }
}');
```

See [Implementation of Custom Algorithms](#implementation-of-custom-algorithms) for a complete example.

### Parameters

| Parameter                    | Required? | Description                                                                                                    |
|------------------------------|-----------|----------------------------------------------------------------------------------------------------------------|
| `<CLASS_NAME>`               | yes       | Name of the query handler class                                                                                |
| `<CLASS_MODULE>`             | yes       | Module name of the query handler class <span style="color: red">(should we mention `builtins` here?)</span>    |
| `<CLASS_PARAMETERS>`         | yes       | Parameters of the query handler class encoded as string                                                        |
| `<UDF_NAME>`                 | -         | Name of Python UDF script including user-implemented algorithm                                                 |
| `<UDF_DB_SCHEMA>`            | -         | Schema name where the UDF script is deployed                                                                   |
| `<BUCKETFS_CONNECTION_NAME>` | yes       | BucketFS connection name to create temporary file outputs                                                      |
| `<BUCKETFS_DIRECTORY>`       | yes       | Directory in BucketFS for temporary file outputs                                                               |
| `<TEMP_DB_SCHEMA>`           | yes       | Database Schema for temporary database objects, e.g. tables                                                    |

# Implementation of Custom Algorithms

Each algorithm should extend the `UDFQueryHandler` abstract class and then implement the following methods:
* `start()`: This method is called at the first execution of the framework, that is, in the first iteration. It returns a result object: Either _Finish_ or _Continue_.
  * The _Finish_ result object contains the final result of the implemented algorithm.
  * The _Continue_ object contains the query list that will be executed for the next state.
* `handle_query_result()`: This method is called at the following iterations to handle the return query.

Here is an example class definition:

```python
--/
CREATE OR REPLACE PYTHON3_AAF SET SCRIPT "MY_SCHEMA"."MY_QUERY_HANDLER_UDF"(...)
EMITS (outputs VARCHAR(2000000)) AS

from typing import Union
from exasol_advanced_analytics_framework.udf_framework.udf_query_handler import UDFQueryHandler
from exasol_advanced_analytics_framework.query_handler.context.query_handler_context import QueryHandlerContext
from exasol_advanced_analytics_framework.query_result.query_result import QueryResult
from exasol_advanced_analytics_framework.query_handler.result import Result, Continue, Finish
from exasol_advanced_analytics_framework.query_handler.query.select_query import SelectQuery, SelectQueryWithColumnDefinition
from exasol_data_science_utils_python.schema.column import Column
from exasol_data_science_utils_python.schema.column_name import ColumnName
from exasol_data_science_utils_python.schema.column_type import ColumnType


-- proposal write parameter into temp table in schema ...
class CustomQueryHandler(UDFQueryHandler):
    def __init__(self, parameter: str, query_handler_context: QueryHandlerContext):
        super().__init__(parameter, query_handler_context)
        self.parameter = parameter
        self.query_handler_context = query_handler_context

    def start(self) -> Union[Continue, Finish[str]]:
        query_list = [
          SelectQuery("SELECT 1 FROM DUAL"),
          SelectQuery("SELECT 2 FROM DUAL")]
        query_handler_return_query = SelectQueryWithColumnDefinition(
            query_string="SELECT 5 AS 'return_column' FROM DUAL",
            output_columns=[
              Column(ColumnName("return_column"), ColumnType("INTEGER"))])

        return Continue(
            query_list=query_list,
            input_query=query_handler_return_query)

    def handle_query_result(self, query_result: QueryResult) -> Union[Continue, Finish[str]]:
        return_value = query_result.return_column
        result = 2 ** return_value
        return Finish(result=f"Assertion of the final result: 32 == {result}")

import builtins
builtins.CustomQueryHandler=CustomQueryHandler # required for pickle

class CustomQueryHandlerFactory:
      def create(self, parameter: str, query_handler_context: QueryHandlerContext):
          return builtins.CustomQueryHandler(parameter, query_handler_context)

builtins.CustomQueryHandlerFactory=CustomQueryHandlerFactory

from exasol_advanced_analytics_framework.udf_framework.query_handler_runner_udf \
    import QueryHandlerRunnerUDF

udf = QueryHandlerRunnerUDF(exa)

def run(ctx):
    return udf.run(ctx)
/


EXECUTE SCRIPT MY_SCHEMA.AAF_RUN_QUERY_HANDLER('{
    "query_handler": {
        "factory_class": {
            "module": "builtins",
            "name": "CustomQueryHandlerFactory"
        },
        "parameters": "bla-bla",
        "udf": {
            "schema": "MY_SCHEMA",
            "name": "MY_QUERY_HANDLER_UDF"
        }
    },
    "temporary_output": {
        "bucketfs_location": {
            "connection_name": "BFS_CON",
            "directory": "temp"
        },
        "schema_name": "TEMP_SCHEMA"
    }
}');
```

The figure below illustrates the execution of this algorithm implemented in class `ExampleQueryHandler`.
* When method `start()` is called, it executes two queries and an additional `input_query` to obtain the next state.
* After the first iteration is completed, the framework calls method `handle_query_result` with the `query_result` of the `input_query` of the previous iteration.

In this example, the algorithm is finished at this state and returns 2<sup>_return value_</sup> as final result.

![Sample Execution](../images/sample_execution.png "Sample Execution")
