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
function M._run_queries(queries, from_index)
    for i=from_index, #queries do
        local success, result = _G.global_env.pquery(result[i])
        if not success then
            local error_obj = exa_error.create(
                    "E-AAF-3",
                    "Error occurred in executing queries: " .. result.error_message)
            _G.global_env.error(tostring(error_obj))
        end
    end
end

---
-- Initiates the Event Loop that handles state transition
--
function M.init(query_to_event_handler)
    local status = "started"

    repeat
        local success, result = _G.global_env.pquery(query_to_event_handler)
        if not success then
            local error_obj = exa_error.create(
                    "E-AAF-2",
                    "Error occurred in calling Event Handler: " .. result.error_message)
            _G.global_env.error(tostring(error_obj))
        end
        query_to_event_handler = result[1][1]
        status = result[2][1]

        M._run_queries(result, 3)
    until (status == 'completed')

    return nil
end

return M;