---
-- @module event_loop
--
-- This module processes only the state transitions by executing queries returned by the Event Handler
--



local M = {}

local exaerror = require("exaerror")


---
-- Executes the given set of queries.
--
function M._run_queries(result, from_index)
    -- TODO
end

---
-- Initiates the Event Loop that handles state transition
--
function M.init(query_to_event_handler)
    local status = "started"

    repeat
        local success, result = pquery(query_to_event_handler) -- TODO
        if not success then
            local error_obj = exaerror.create() -- TODO
            error(tostring(error_obj))
        end
        query_to_event_handler = result[1][1] -- TODO
        status = result[2][1]

        M._run_queries(result, 3)
    until (status == 'completed')

    return nil
end

return M;