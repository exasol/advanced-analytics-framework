---
-- @module exa_script_tools
--
-- This module contains utilities for Lua Scripts in Exasol
--

local M = {
}

local ExaError = require("ExaError")
local json = require("cjson")

---
-- Extend `exa-object` with the global functions available in Lua Scripts.
-- See Exasol Scripting: https://docs.exasol.com/db/latest/database_concepts/scripting/db_interaction.htm
-- We provide this function such that implementers of a main function can encapsulate the global objects
-- and properly inject them into modules.
--
-- @param exa exa-object available inside of Lua Scripts
--
-- @return lua table including meta and functions
--
function M.create_exa_env(exa)
    local exa_env <const> = {
        meta = exa.meta,
        -- We put the global functions into a subtable, such that we can replace the subtable with a mock
        functions = {
            pquery = pquery,
            query = query,
            error = error
        }
    }
    return exa_env
end

---
-- Parse a given arguments in JSON string format.
--
-- @param json_str input parameters as JSON string
--
-- @return Lua table containing the parameters
--
function M.parse_arguments(json_str, exa_env)
    local success, args = pcall(json.decode, json_str)
    if not success then
        local error_obj <const> = ExaError:new(
                "E-AAF-1",
                "Arguments could not be converted from JSON object to Lua table: {{raw_json}}",
                { raw_json = { value = json_str, description = "raw JSON object" } },
                { "Check syntax of the input string JSON is correct" }
        )
        exa_env.functions.error(tostring(error_obj))
    end
    return args
end

---
-- Encapsulates the result of a QueryHandler such that it can be returns from a Exasol Lua Script.
--
-- @param result A string containing the result of the QueryHandler
--
-- @return A tuple of a table with a single row and one column and the SQL column definition for it
--
function M.wrap_result(result)
    local return_result <const> = { { result } }
    return return_result, "result_column VARCHAR(2000000)"
end

return M