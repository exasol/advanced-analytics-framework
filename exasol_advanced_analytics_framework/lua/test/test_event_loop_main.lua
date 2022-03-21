local luaunit = require("luaunit")
local mockagne = require("mockagne")
local event_loop_main = require("event_loop_main")

test_event_loop_main = {
    correct_json_str = [[
    {
        "action" : "action",
        "parameter" : "parameters",
        "schema" : "SCHEMA",
        "sql_prefix" : "PRE"
    }]],
    incorrect_json_str = [[
    {
        "action" : "action",
        "parameter" : "parameters"
    ]],
    args = {
        action = "action",
        parameters = "parameters",
        schema = "SCHEMA",
        sql_prefix = "PRE"
    },
    query = "SELECT SCHEMA.PRE_EVENT_HANDLER_RUNNER_UDF('action','parameters')"
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
    local args = event_loop_main._parse_arguments(
            test_event_loop_main.correct_json_str)
    luaunit.assertNotNil(args["action"])
    luaunit.assertNotNil(args["parameter"])
    luaunit.assertNotNil(args["schema"])
    luaunit.assertNotNil(args["sql_prefix"])
end

function test_event_loop_main.test_parse_incorrect_json()
    local args = event_loop_main._parse_arguments(
            test_event_loop_main.incorrect_json_str)
    luaunit.assertNil(args["action"])
    luaunit.assertNil(args["parameter"])
    luaunit.assertNil(args["schema"])
    luaunit.assertNil(args["sql_prefix"])
end

function test_event_loop_main.test_prepare_init_query()
    local query = event_loop_main._prepare_init_query(test_event_loop_main.args)
    luaunit.assertEquals(query, test_event_loop_main.query)
end

os.exit(luaunit.LuaUnit.run())

