local luaunit = require("luaunit")
local mockagne = require("mockagne")
local query_loop = require("query_loop")

local function mock_pquery_add_queries(exa_mock, query_list)
    for i = 1, #query_list do
        mockagne.when(exa_mock.pquery(query_list[i], _)).thenAnswer(true, nil)
    end
end

local function mock_pquery_verify_queries(exa_mock, query_list)
    for i = 1, #query_list do
        mockagne.verify(exa_mock.pquery(query_list[i], _))
    end
end

function make_query_table(query_list)
    local result = {}
    for i = 1, #query_list do
        table.insert(result, { query_list[i] })
    end
    return result
end

function concat_list(list1, list2)
    local result = {}
    for i = 1, #list1 do
        table.insert(result, list1[i])
    end
    for i = 1, #list2 do
        table.insert(result, list2[i])
    end
    return result
end

test_query_loop = {
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
        query = "SELECT UDF_SCHEMA.UDF_NAME(" ..
                "0,'bfs_conn','directory','db_name_1122334455_1','temp_schema'," ..
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
        query = "SELECT script_schema.AAF_QUERY_HANDLER_UDF(" ..
                "0,'bfs_conn','directory','db_name_1122334455_1','temp_schema'," ..
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

function test_query_loop.test_run_queries_without_skip()
    local functions_mock = mockagne.getMock()
    local exa_env = {
        functions = functions_mock
    }
    local query_list = {
        "SELECT QUERY1()",
        "SELECT QUERY2()",
        "SELECT QUERY3()",
    }
    mock_pquery_add_queries(functions_mock, query_list)
    local query_table = make_query_table(query_list)
    local result = query_loop._run_queries(query_table, 1, exa_env)
    mock_pquery_verify_queries(functions_mock, query_list)
    luaunit.assertEquals(result, nil)
end

function test_query_loop.test_run_queries_with_skip()
    local functions_mock = mockagne.getMock()
    local exa_env = {
        functions = functions_mock
    }
    local things_to_skip = {
        "DO NOT EXECUTE 1",
        "DO NOT EXECUTE 2"
    }
    local query_list = {
        "SELECT QUERY1()",
        "SELECT QUERY2()",
        "SELECT QUERY3()",
    }
    mock_pquery_add_queries(functions_mock, query_list)
    local complete_query_list = concat_list(things_to_skip, query_list)
    local query_table = make_query_table(complete_query_list)
    local result = query_loop._run_queries(query_table, 3, exa_env)
    mock_pquery_verify_queries(functions_mock, query_list)
    luaunit.assertEquals(result, nil)
end

function test_query_loop.test_init_single_iteration_finished_without_cleanup()
    local functions_mock = mockagne.getMock()
    local exa_env = {
        functions = functions_mock
    }
    local init_query = "SELECT AAF_QUERY_HANDLER_UDF(0)"
    local init_query_result = {
        { nil },
        { nil },
        { "FINISHED" },
        { "final_result" }
    }
    mockagne.when(functions_mock.pquery(init_query, _)).thenAnswer(true, init_query_result)
    local result = query_loop.run(init_query, exa_env)
    mock_pquery_verify_queries(functions_mock, { init_query })
    luaunit.assertEquals(result, "final_result")
end

function test_query_loop.test_init_single_iteration_finished_with_cleanup()
    local functions_mock = mockagne.getMock()
    local exa_env = {
        functions = functions_mock
    }
    local init_query = "SELECT AAF_QUERY_HANDLER_UDF(0)"
    local cleanup_query = "DROP TABLE test;"
    local init_query_result = {
        { nil },
        { nil },
        { "FINISHED" },
        { "final_result" },
        { cleanup_query }
    }
    mockagne.when(functions_mock.pquery(init_query, _)).thenAnswer(true, init_query_result)
    mockagne.when(functions_mock.pquery(cleanup_query, _)).thenAnswer(true, nil)
    local result = query_loop.run(init_query, exa_env)
    mock_pquery_verify_queries(functions_mock, {
        init_query, cleanup_query
    })
    luaunit.assertEquals(result, "final_result")
end

function test_query_loop.test_init_two_iteration_finished_without_cleanup()
    local functions_mock = mockagne.getMock()
    local exa_env = {
        functions = functions_mock
    }
    local init_query = "SELECT AAF_QUERY_HANDLER_UDF(0)"
    local return_query_view = "CREATE VIEW return_query_view"
    local return_query = "SELECT AAF_QUERY_HANDLER_UDF(1) FROM return_query_view"
    local query_list_returned_by_init_query = {
        "SELECT QUERY1()",
        "SELECT QUERY2()",
        "SELECT QUERY3()",
    }
    local init_query_result_begin = {
        { return_query_view },
        { return_query },
        { "CONTINUE" },
        { "{}" }
    }
    local return_query_result = {
        { nil },
        { nil },
        { "FINISHED" },
        { "final_result" },
    }
    query_table_returned_by_init_query = make_query_table(query_list_returned_by_init_query)
    local init_query_result = concat_list(init_query_result_begin, query_table_returned_by_init_query)
    mockagne.when(functions_mock.pquery(init_query, _)).thenAnswer(true, init_query_result)
    mock_pquery_add_queries(functions_mock, query_list_returned_by_init_query)
    mockagne.when(functions_mock.pquery(return_query_view, _)).thenAnswer(true, nil)
    mockagne.when(functions_mock.pquery(return_query, _)).thenAnswer(true, return_query_result)
    local result = query_loop.run(init_query, exa_env)
    mock_pquery_verify_queries(functions_mock, { init_query })
    mock_pquery_verify_queries(functions_mock, query_list_returned_by_init_query)
    mock_pquery_verify_queries(functions_mock, { return_query_view, return_query })
    luaunit.assertEquals(result, "final_result")
end

function test_query_loop.test_init_two_iteration_finished_with_cleanup()
    local functions_mock = mockagne.getMock()
    local exa_env = {
        functions = functions_mock
    }
    local init_query = "SELECT AAF_QUERY_HANDLER_UDF(0)"
    local return_query_view = "CREATE VIEW return_query_view"
    local return_query = "SELECT AAF_QUERY_HANDLER_UDF(1) FROM return_query_view"
    local cleanup_query = "DROP TABLE test;"
    local query_list_returned_by_init_query = {
        "SELECT QUERY1()",
        "SELECT QUERY2()",
        "SELECT QUERY3()",
    }
    local init_query_result_begin = {
        { return_query_view },
        { return_query },
        { "CONTINUE" },
        { "{}" }
    }
    local return_query_result = {
        { nil },
        { nil },
        { "FINISHED" },
        { "final_result" },
        { cleanup_query }
    }
    query_table_returned_by_init_query = make_query_table(query_list_returned_by_init_query)
    local init_query_result = concat_list(init_query_result_begin, query_table_returned_by_init_query)
    mockagne.when(functions_mock.pquery(init_query, _)).thenAnswer(true, init_query_result)
    mock_pquery_add_queries(functions_mock, query_list_returned_by_init_query)
    mockagne.when(functions_mock.pquery(return_query_view, _)).thenAnswer(true, nil)
    mockagne.when(functions_mock.pquery(return_query, _)).thenAnswer(true, return_query_result)
    mockagne.when(functions_mock.pquery(cleanup_query, _)).thenAnswer(true, nil)
    local result = query_loop.run(init_query, exa_env)
    mock_pquery_verify_queries(functions_mock, { init_query })
    mock_pquery_verify_queries(functions_mock, query_list_returned_by_init_query)
    mock_pquery_verify_queries(functions_mock, { return_query_view, return_query })
    mock_pquery_verify_queries(functions_mock, { cleanup_query })
    luaunit.assertEquals(result, "final_result")
end

function test_query_loop.test_init_single_iteration_error_with_cleanup()
    local functions_mock = mockagne.getMock()
    local exa_env = {
        functions = functions_mock
    }
    local init_query = "SELECT AAF_QUERY_HANDLER_UDF(0)"
    local cleanup_query = "DROP TABLE test;"
    local error_message = "Error Message"
    local expected_error = "E-AAF-4: Error occurred during running the QueryHandlerUDF: " .. error_message
    local init_query_result = {
        { nil },
        { nil },
        { "ERROR" },
        { error_message },
        { cleanup_query }
    }
    mockagne.when(functions_mock.pquery(init_query, _)).thenAnswer(true, init_query_result)
    mockagne.when(functions_mock.pquery(cleanup_query, _)).thenAnswer(true, nil)
    query_loop.run(init_query, exa_env)
    mock_pquery_verify_queries(functions_mock, {
        init_query, cleanup_query
    })
    mockagne.verify(functions_mock.error(expected_error, _))
end

function test_query_loop.test_init_single_iteration_query_error()
    local functions_mock = mockagne.getMock()
    local exa_env = {
        functions = functions_mock
    }
    local init_query = "SELECT AAF_QUERY_HANDLER_UDF(0)"
    local error_message = "Error Message"
    local expected_error = "E-AAF-3: Error occurred in executing the query: " ..
            "SELECT AAF_QUERY_HANDLER_UDF(0) error message: " .. error_message
    local init_query_result = {
        { nil },
        { nil },
        { "FINISHED" },
        { "" },
        error_message = error_message }
    mockagne.when(functions_mock.pquery(init_query, _)).thenAnswer(false, init_query_result)
    query_loop.run(init_query, exa_env)
    mock_pquery_verify_queries(functions_mock, { init_query })
    mockagne.verify(functions_mock.error(expected_error, _))
end

function test_query_loop.test_prepare_init_query_correct_with_udf()
    local meta = {
        database_name = "db_name",
        session_id = "1122334455",
        statement_id = "1"
    }
    local query = query_loop.prepare_init_query(test_query_loop.correct_with_udf.args, meta)
    luaunit.assertEquals(query, test_query_loop.correct_with_udf.query)
end

function test_query_loop.test_prepare_init_query_correct_without_udf()
    local meta = {
        database_name = "db_name",
        session_id = "1122334455",
        statement_id = "1",
        script_schema = "script_schema"
    }
    local query = query_loop.prepare_init_query(test_query_loop.correct_without_udf.args, meta)
    luaunit.assertEquals(query, test_query_loop.correct_without_udf.query)
end

function test_query_loop.test_prepare_init_query_incorrect_without_class()
    local meta = {
        database_name = "db_name",
        session_id = "1122334455",
        statement_id = "1",
        script_schema = "script_schema"
    }
    luaunit.assertError(query_loop.prepare_init_query, test_query_loop.incorrect_without_class.args, meta)
end

function test_query_loop.test_prepare_init_query_incorrect_without_temporary_output()
    local meta = {
        database_name = "db_name",
        session_id = "1122334455",
        statement_id = "1",
        script_schema = "script_schema"
    }
    luaunit.assertError(query_loop.prepare_init_query, test_query_loop.incorrect_without_temporary_output.args, meta)
end

os.exit(luaunit.LuaUnit.run())
