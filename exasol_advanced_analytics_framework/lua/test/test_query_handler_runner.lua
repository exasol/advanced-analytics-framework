local json = require("cjson")
local luaunit = require("luaunit")
local query_handler_runner = require("query_handler_runner")

test_query_handler_runner = {
    correct_with_udf = {
        args = {
            temporary_output = {
                bucketfs_location = {
                    connection_name = "bfs_conn",
                    directory = "directory"
                },
                schema_name = "temp_schema"
            },
            query_handler = {
                factory_class = {
                    name = "cls_name",
                    module = "package.module"
                },
                udf = {
                    schema = "UDF_SCHEMA",
                    name = "UDF_NAME"
                },
                parameters = "params"
            },
        },
        query = "SELECT UDF_SCHEMA.UDF_NAME(" ..
                "0,'bfs_conn','directory','db_name_1122334455_1','temp_schema'," ..
                "'cls_name','package.module','params')",
        return_query_result = {
            { nil },
            { nil },
            { "FINISHED" },
            { "final_result" },
        },
        query_handler_result = "final_result"
    },
}

function test_query_handler_runner.test_query_handler_runner()
    query_handler_runner._exasol_script_tools.create_exa_env = function(exa)
        return {
            meta = exa.meta,
            functions = {
                error = function(_,_)
                    luaunit.fail("error called")
                end,
                pquery = function(query, _)
                    luaunit.assertEquals(query, test_query_handler_runner.correct_with_udf.query)
                    actual_result = test_query_handler_runner.correct_with_udf.return_query_result
                    return true, actual_result
                end,
                query = function(_)
                    luaunit.fail("query called")
                end,
            }
        }
    end
    local exa = {
        meta = {
            script_schema = "test_schema",
            database_name = "db_name",
            session_id = "1122334455",
            statement_id = "1"
        }
    }
    local json_str = json.encode(test_query_handler_runner.correct_with_udf.args)
    local actual_result, actual_column_definition = query_handler_runner.run(json_str, exa)
    local expected_result = { { test_query_handler_runner.correct_with_udf.query_handler_result } }
    local expected_column_definition = "result_column VARCHAR(2000000)"
    luaunit.assertEquals(actual_result, expected_result)
    luaunit.assertEquals(actual_column_definition, expected_column_definition)
end

os.exit(luaunit.LuaUnit.run())
