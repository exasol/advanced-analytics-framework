local luaunit = require("luaunit")
local mockagne = require("mockagne")
require("query_handler_runner_main")

test_query_handler_runner_main = {
}

function test_query_handler_runner_main.test_query_handler_runner()
    query_handler_runner = mockagne.getMock()
    json_str = "{}"
    exa = { meta = {} }
    expected_result = "result"
    mockagne.when(query_handler_runner.run(json_str, exa)).thenAnswer(expected_result)
    actual_result = query_handler_runner_main(json_str, exa)
    mockagne.verify(query_handler_runner.run(json_str, exa))
    luaunit.assertEquals(actual_result, expected_result)

end

os.exit(luaunit.LuaUnit.run())
