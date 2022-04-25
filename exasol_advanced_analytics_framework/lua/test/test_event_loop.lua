local luaunit = require("luaunit")
local mockagne = require("mockagne")
local event_loop = require("event_loop")

test_event_loop = {
    result = {
        {"col1,col2,col3"},
        {"completed"},
        {"SELECT QUERY1()"},
        {"SELECT QUERY2()"},
        {"SELECT QUERY3()"},
    },
    schema = "TEST_SCHEMA",
    class_name = "class_name",
    buckefs_connection = "bfs_conn",
    parameters = ""
}

local function mock_error_return_nil(exa_mock)
    mockagne.when(exa_mock.error()).thenAnswer(nil)
end

local function mock_pquery_event_handler_query(exa_mock, query)
    mockagne.when(exa_mock.pquery(query, _)).thenAnswer(true, test_event_loop.result)
end

local function mock_pquery_queries(exa_mock)
    local query_list = {
        test_event_loop.result[3],
        test_event_loop.result[4],
        test_event_loop.result[5]
    }
    for i=1, #query_list do
        mockagne.when(exa_mock.pquery(query_list[i][1], _)).thenAnswer(true, nil)
    end
end

function  test_event_loop.setUp()
    exa_mock = mockagne.getMock()
    _G.global_env = exa_mock
    mock_error_return_nil(exa_mock)
end

function test_event_loop.test_get_udf_query_components()
    local variadic_params, with_clause = event_loop.__get_udf_query_components(
            test_event_loop.result[1][1])
    luaunit.assertEquals(variadic_params,
            ",col1,col2,col3")
    luaunit.assertEquals(with_clause,
            "WITH return_query(col1,col2,col3) %s FROM return_query")
end

function test_event_loop.test_prepare_event_handler_udf_first_iteration()
    local query = event_loop._prepare_event_handler_udf(
            0,
            nil,
            test_event_loop.schema,
            test_event_loop.class_name,
            test_event_loop.buckefs_connection,
            test_event_loop.parameters
    )
    local expected_query = string.format(
            "SELECT %s.EVENTHANDLERUDF(0, '%s','%s' )",
            test_event_loop.schema,
            test_event_loop.class_name,
            test_event_loop.buckefs_connection)
    luaunit.assertEquals(query, expected_query)
end


function test_event_loop.test_prepare_event_handler_udf_with_column_names()
    local query = event_loop._prepare_event_handler_udf(
            1,
            test_event_loop.result[1][1],
            test_event_loop.schema,
            test_event_loop.class_name,
            test_event_loop.buckefs_connection,
            test_event_loop.parameters
    )
    local expected_query = string.format(
            "WITH return_query(col1,col2,col3) SELECT %s.EVENTHANDLERUDF(1, '%s','%s' ) FROM return_query",
            test_event_loop.schema,
            test_event_loop.class_name,
            test_event_loop.buckefs_connection)
    luaunit.assertEquals(query, expected_query)
end

function test_event_loop.test_run_queries()
    mock_pquery_queries(exa_mock)
    local all_success = event_loop._run_queries(
            test_event_loop.result, 3)
    luaunit.assertEquals(all_success, true)
end

function test_event_loop.test_init()
    local query = string.format(
        "SELECT %s.EVENTHANDLERUDF(0, '%s','%s' )",
        test_event_loop.schema,
        test_event_loop.class_name,
        test_event_loop.buckefs_connection)
    mock_pquery_queries(exa_mock)
    mock_pquery_event_handler_query(exa_mock, query)
    local status = event_loop.init(
            test_event_loop.schema,
            test_event_loop.class_name,
            test_event_loop.buckefs_connection,
            test_event_loop.parameters
    )
    luaunit.assertEquals(status, "completed")
end


os.exit(luaunit.LuaUnit.run())