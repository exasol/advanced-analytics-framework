local luaunit = require("luaunit")
local mockagne = require("mockagne")
local json = require('cjson')
require("query_loop_main")

test_query_loop_main = {
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
        query = "SELECT UDF_SCHEMA.UDF_NAME("..
                "0,'bfs_conn','directory','db_name_1122334455_1','temp_schema',"..
                "'cls_name','package.module','params')"
    },
    correct_without_udf = {
        args = {
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
                parameters = "params"
            },
        },
        query = "SELECT script_schema.AAF_QUERY_HANDLER_UDF("..
                "0,'bfs_conn','directory','db_name_1122334455_1','temp_schema',"..
                "'cls_name','package.module','params')"
    },
    incorrect_without_class = {
        args = {
            query_handler = {
                class = {
                    name = "cls_name",
                    module = "package.module"
                },
                parameters = "params"
            },
        },
    },
    incorrect_without_temporary_output = {
        args = {
            temporary_output = {
                bucketfs_location = {
                    connection_name = "bfs_conn",
                    directory = "directory"
                },
                schema_name = "temp_schema"
            },
            query_handler = {
                parameters = "params"
            },
        },
    }
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
    local expected_table = test_query_loop_main.correct_with_udf.args
    local json_str = json.encode(expected_table)
    local args = _parse_arguments(json_str)
    luaunit.assertEquals(args, expected_table)
end

function test_query_loop_main.test_parse_arguments_query_correct_without_udf()
    local expected_table = test_query_loop_main.correct_without_udf.args
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

function test_query_loop_main.test_prepare_init_query_correct_with_udf()
    local meta = {
        database_name = "db_name",
        session_id = "1122334455",
        statement_id = "1"
    }
    local query = _prepare_init_query(test_query_loop_main.correct_with_udf.args, meta)
    luaunit.assertEquals(query, test_query_loop_main.correct_with_udf.query)
end

function test_query_loop_main.test_prepare_init_query_correct_without_udf()
    local meta = {
        database_name = "db_name",
        session_id = "1122334455",
        statement_id = "1",
        script_schema = "script_schema"
    }
    local query = _prepare_init_query(test_query_loop_main.correct_without_udf.args, meta)
    luaunit.assertEquals(query, test_query_loop_main.correct_without_udf.query)
end

function test_query_loop_main.test_prepare_init_query_incorrect_without_class()
    local meta = {
        database_name = "db_name",
        session_id = "1122334455",
        statement_id = "1",
        script_schema = "script_schema"
    }
    luaunit.assertError(_prepare_init_query, test_query_loop_main.incorrect_without_class.args, meta)
end

function test_query_loop_main.test_prepare_init_query_incorrect_without_temporary_output()
    local meta = {
        database_name = "db_name",
        session_id = "1122334455",
        statement_id = "1",
        script_schema = "script_schema"
    }
    luaunit.assertError(_prepare_init_query, test_query_loop_main.incorrect_without_temporary_output.args, meta)
end

os.exit(luaunit.LuaUnit.run())
