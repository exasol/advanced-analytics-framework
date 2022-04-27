local luaunit = require("luaunit")
local mockagne = require("mockagne")
local event_loop = require("event_loop")

test_event_loop = {
    result = {
        {"CREATE VIEW AS TMP_VIEW SELECT RETURN_QUERY"},
        {"SELECT AAF_EVENT_HANDLER_UDF"},
        {"completed"},
        {"SELECT QUERY1()"},
        {"SELECT QUERY2()"},
        {"SELECT QUERY3()"},
    }
}

local function mock_error_return_nil(exa_mock)
    mockagne.when(exa_mock.error()).thenAnswer(nil)
end

local function mock_pquery_event_handler_query(exa_mock, query)
    mockagne.when(exa_mock.pquery(query, _)).thenAnswer(true, test_event_loop.result)
end

local function mock_pquery_queries(exa_mock)
    local query_list = {
        test_event_loop.result[4],
        test_event_loop.result[5],
        test_event_loop.result[6]
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

function test_event_loop.test_run_queries()
    mock_pquery_queries(exa_mock)
    local result = event_loop._run_queries(
            test_event_loop.result, 4)
    luaunit.assertEquals(result, nil)
end

function test_event_loop.test_init()
    mock_pquery_queries(exa_mock)
    mock_pquery_event_handler_query(exa_mock, test_event_loop.result[1][1])
    mock_pquery_event_handler_query(exa_mock, test_event_loop.result[2][1])
    local status = event_loop.init(test_event_loop.result[2][1])
    luaunit.assertEquals(status, "completed")
end


os.exit(luaunit.LuaUnit.run())