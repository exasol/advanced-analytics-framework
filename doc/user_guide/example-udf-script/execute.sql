EXECUTE SCRIPT "AAF_DB_SCHEMA"."AAF_RUN_QUERY_HANDLER"('{
    "query_handler": {
        "factory_class": {
            "module": "example_module",
            "name": "ExampleQueryHandlerFactory"
        },
        "parameter": "bla-bla",
        "udf": {
            "schema": "EXAMPLE_SCHEMA",
            "name": "EXAMPLE_QUERY_HANDLER_UDF"
        }
    },
    "temporary_output": {
        "bucketfs_location": {
            "connection_name": "EXAMPLE_BFS_CON",
            "directory": "temp"
        },
        "schema_name": "EXAMPLE_TEMP_SCHEMA"
    }
}')
