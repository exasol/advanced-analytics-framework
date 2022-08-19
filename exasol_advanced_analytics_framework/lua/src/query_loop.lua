---
-- @module query_loop
--
-- This module processes only the state transitions by executing queries returned by the Query Handler
--

local M = {
}
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
    for i = from_index, #queries do
        local query = queries[i][1]
        if query ~= nil then
            success, result = _G.global_env.pquery(query)
            if not success then
                -- TODO cleanup after query error
                local error_obj = exa_error.create(
                        "E-AAF-3",
                        "Error occurred in executing the query: "
                                .. query
                                .. " error message: "
                                .. result.error_message)
                _G.global_env.error(tostring(error_obj))
            end
        end
    end
    return result
end

---
-- Initiates the Query Loop that handles state transition
--
-- @param query string that calls the query handler
--
function M.init(query_to_query_handler)
    local status = false
    local final_result_or_error
    local query_to_create_view
    repeat
        -- call QueryHandlerUDF return queries
        local return_queries = { { query_to_create_view }, { query_to_query_handler } }
        local result = M._run_queries(return_queries, 1)

        -- handle QueryHandlerUDF return
        query_to_create_view = result[1][1]
        query_to_query_handler = result[2][1]
        status = result[3][1]
        final_result_or_error = result[4][1]
        M._run_queries(result, 5)
    until (status ~= 'CONTINUE')
    if status == 'ERROR' then
        local error_obj = exa_error.create(
                "E-AAF-4",
                "Error occurred during running the QueryHandlerUDF: "
                        .. final_result_or_error)
        _G.global_env.error(tostring(error_obj))
    end
    return final_result_or_error
end

return M;