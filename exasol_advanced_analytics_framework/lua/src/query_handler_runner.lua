---
-- @module query_handler_runner
--
-- This modules includes the run function of the Query Loop
--

M = {
    _query_loop = require("query_loop"),
    _exasol_script_tools = require("exasol_script_tools")
}

function M.run(json_str, exa)
    local exa_env <const> = M._exasol_script_tools.create_exa_env(exa)
    local args <const> = M._exasol_script_tools.parse_arguments(json_str)
    local init_query <const> = M._query_loop.prepare_init_query(args, exa_env.meta)
    local result <const> = M._query_loop.run(init_query, exa_env)
    return M._exasol_script_tools.wrap_result(result)
end

return M