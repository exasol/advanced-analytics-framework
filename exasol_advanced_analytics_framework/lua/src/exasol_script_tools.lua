---
-- @module exa_script_tools
--
-- This module contains utilities for Lua Scripts in Exasol
--

local M = {
}

local exaerror = require("exaerror")
local json = require('cjson')

---
-- Extend exa-object with the global functions available in Lua Scripts
--
-- @param exa exa-object available inside of Lua Scripts
--
-- @return lua table including meta and functions
--
function M.create_exa_env(exa)
    local exa_env = {
        meta = exa.meta,
        functions = {
            pquery = pquery,
            query = query,
            error = error
        }
    }
    return exa_env
end

---
-- Parse a given arguments in json string format.
--
-- @param json_str input parameters as json string
--
-- @return lua table including parameters
--
function M.parse_arguments(json_str, exa_env)
    local success, args = pcall(json.decode, json_str)
    if not success then
        local error_obj = exaerror.create(
                "E-AAF-1",
                "It could not be converted to json object"
        )                         :add_mitigations("Check syntax of the input string json is correct")
        exa_env.functions.error(tostring(error_obj))
    end
    return args
end

function M.wrap_result(result)
    local return_result = { { result } }
    return return_result, "result_column VARCHAR(2000000)"
end

return M