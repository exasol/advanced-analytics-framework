local luaunit = require("luaunit")
local mockagne = require("mockagne")
local json = require('cjson')
require("query_loop_main")

test_query_loop_main = {
    correct_json = {
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
    },
}

local function mock_error_return_nil(exa_mock)
    mockagne.when(exa_mock.error()).thenAnswer(nil)
end

function test_query_loop_main.setUp()
    exa_mock = mockagne.getMock()
    _G.global_env = exa_mock
    mock_error_return_nil(exa_mock)
end

function test_query_loop_main.test_parse_arguments_query_correct_with_udf()
    local expected_table = test_query_loop_main.correct_json
    local json_str = json.encode(expected_table)
    local args = _parse_arguments(json_str)
    luaunit.assertEquals(args, expected_table)
end

function test_query_loop_main.test_parse_arguments_query_incorrect_json()
    local json_str = [[{ "abc ": "bc", "cde"}]]
    _parse_arguments(json_str)
    local expected_error = [[E-AAF-1: It could not be converted to json object

Mitigations:

* Check syntax of the input string json is correct]]
    mockagne.verify(exa_mock.error(expected_error))
end

os.exit(luaunit.LuaUnit.run())
