---
-- @module event_loop
--
-- This module processes only the state transitions by executing queries returned by the Event Handler
--

local M = {}
local exa_error = require("exaerror")

_G.global_env = {
    pquery = pquery,
    error = error
}

---
-- Executes the given set of queries.
--
-- @param   queries lua table including queries
-- @param   from_index the index where the queries in the lua table start
--
-- @return  the result of the latest query
--
function M._run_queries(queries, from_index)
    for i=from_index, #queries do
        if queries[i][1] ~= nil then
            success, result = _G.global_env.pquery(queries[i][1])
            if not success then
                local error_obj = exa_error.create(
                        "E-AAF-3",
                        "Error occurred in executing the query: "
                                .. queries[i][1]
                                .. " error message: "
                                .. result.error_message)
                _G.global_env.error(tostring(error_obj))
            end
        end
    end
    return result
end

---
-- Initiates the Event Loop that handles state transition
--
-- @param query string that calls the event handler
--
function M.init(query_to_event_handler)
    local status = "started"
    local final_result = nil
    local query_to_create_view = nil
    repeat
        -- call EventHandlerUDF return queries
        local return_queries = {{query_to_create_view}, {query_to_event_handler}}
        local result = M._run_queries(return_queries, 1)

        -- handle EventHandlerUDF return
        query_to_create_view = result[1][1]
        query_to_event_handler = result[2][1]
        status = result[3][1]
        final_result = result[4][1]
        M._run_queries(result, 5)
    until (status == 'finished')

    return final_result
end


return M;