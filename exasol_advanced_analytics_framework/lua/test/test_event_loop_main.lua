local luaunit = require("luaunit")
local mockagne = require("mockagne")
require("event_loop_main")

test_event_loop_main = {
    correct_json_str = [[
    {
        "schema"                    : "SCHEMA",
        "bucketfs_connection"       : "bfs_conn",
        "event_handler_class"       : "cls_name",
        "event_handler_parameters"  : "params"
    }]],
    incorrect_json_str = [[
    {
        "event_handler_class"       : "event_handler_class_name",
        "event_handler_parameters"  : "params"
    ]],
    args = {
        schema                      = "SCHEMA",
        bucketfs_connection         = "bfs_conn",
        event_handler_class         = "cls_name",
        event_handler_parameters    = "params"
    },
    query = "SELECT SCHEMA.AAF_EVENT_HANDLER_UDF(0,'bfs_conn','cls_name','params')"
}

local function mock_error_return_nil(exa_mock)
    mockagne.when(exa_mock.error()).thenAnswer(nil)
end

function  test_event_loop_main.setUp()
    exa_mock = mockagne.getMock()
    _G.global_env = exa_mock
    mock_error_return_nil(exa_mock)
end

function test_event_loop_main.test_parse_correct_json()
    local args = _parse_arguments(test_event_loop_main.correct_json_str)
    luaunit.assertNotNil(args["schema"])
    luaunit.assertNotNil(args["bucketfs_connection"])
    luaunit.assertNotNil(args["event_handler_class"])
    luaunit.assertNotNil(args["event_handler_parameters"])
end

function test_event_loop_main.test_parse_incorrect_json()
    local args = _parse_arguments(test_event_loop_main.incorrect_json_str)
    luaunit.assertNil(args["schema"])
    luaunit.assertNil(args["bucketfs_connection"])
    luaunit.assertNil(args["event_handler_class"])
    luaunit.assertNil(args["event_handler_parameters"])
end

function test_event_loop_main.test_prepare_init_query()
    local query = _prepare_init_query(test_event_loop_main.args)
    luaunit.assertEquals(query, test_event_loop_main.query)
end

os.exit(luaunit.LuaUnit.run())
