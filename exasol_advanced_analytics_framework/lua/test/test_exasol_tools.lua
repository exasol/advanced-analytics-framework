local json = require("cjson")
local luaunit = require("luaunit")
local exasol_script_tools = require("exasol_script_tools")
local mockagne = require("mockagne")

test_exasol_script_tools = {
}

function test_exasol_script_tools.test_parse_arguments_query_correct_with_udf()
    local functions_mock = mockagne.getMock()
    exa_env = {
        functions = functions_mock
    }
    local expected_table = {
        temporary_output = {
            bucketfs_location = {
                connection_name = "bfs_conn",
                directory = "directory"
            },
            schema_name = "temp_schema"
        },
        query_handler = {
            class = {
                name = "cls_name",
                module = "package.module"
            },
            udf = {
                schema = "UDF_SCHEMA",
                name = "UDF_NAME"
            },
            parameters = "params"
        },
    }
    local json_str = json.encode(expected_table)
    local args = exasol_script_tools.parse_arguments(json_str, exa_env)
    luaunit.assertEquals(args, expected_table)
end

function test_exasol_script_tools.test_parse_arguments_query_incorrect_json()
    local functions_mock = mockagne.getMock()
    local exa_env = {
        functions = functions_mock
    }
    local json_str = [[{ "abc ": "bc", "cde"}]]
    exasol_script_tools.parse_arguments(json_str, exa_env)
    local expected_error = [[E-AAF-1: It could not be converted to json object

Mitigations:

* Check syntax of the input string json is correct]]
    mockagne.verify(functions_mock.error(expected_error))
end

function test_exasol_script_tools.test_wrap_result()
    local json_str = [[{ "abc ": "bc", "cde"}]]
    actual_result, actual_column_definition = exasol_script_tools.wrap_result(json_str)
    expected_result = { { json_str } }
    expected_column_definition = "result_column VARCHAR(2000000)"
    luaunit.assertEquals(actual_result, expected_result)
    luaunit.assertEquals(actual_column_definition, expected_column_definition)
end


os.exit(luaunit.LuaUnit.run())