---
-- @module query_loop_main
--
-- This script contains the main function of the Query Loop.
--

query_handler_runner = require("query_handler_runner")
---
-- This is the main function of the Query Loop.
--
-- @param json_str	input parameters as JSON string
-- @param exa	the database context (`exa`) of the Lua script
--
function query_handler_runner_main(json_str, exa)
    return query_handler_runner.run(json_str, exa)
end

