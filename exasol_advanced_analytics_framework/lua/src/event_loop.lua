---
-- @module event_loop
--
-- This module processes only the state transitions by executing queries returned by the Event Handler
--

local M = {
    event_handler_udf_name = string.upper("EventHandlerUDF")
}
local exa_error = require("exaerror")

_G.global_env = {
    pquery = pquery,
    error = error
}

---
-- Returns variadic arguments and WITH clause statement of the UDF query
--
-- @param column_names  comma-separated names of columns that EventHandlerUDF uses
--
-- @return string of variadic arguments and WITH clause statement
--
function M.__get_udf_query_components(column_names)
    local variadic_params = ""
    local with_clause = '%s'
    if column_names ~= '' and column_names ~= nil then
        variadic_params = string.format(",%s", column_names)
        with_clause = string.format("WITH return_query(%s)", column_names)
                .. " %s " .. "FROM return_query"
    end
    return variadic_params, with_clause
end


---
-- Prepares query that calls the EventHandlerUDF
--
-- @param iter_num      the integer showing the iteration number
-- @param column_names  comma-separated names of columns that EventHandlerUDF uses
-- @param schema        name of schema where the EventHandlerUDF is installed
-- @param class_name    name of the class built on the Event-Handler
-- @param bucketfs_conn the name of BucketFS connection
-- @parameters          config parameters of the Event-Handler
--
-- @return EventHandlerUDF query string
--
function M._prepare_event_handler_udf(
        iter_num, column_names, schema, class_name, bucketfs_conn, parameters)

    local _variadic_params, _with_clause = M.__get_udf_query_components(column_names)
    local _udf_name = string.format("%s.%s", schema, M.event_handler_udf_name)
    local _udf_args = string.format("(%d, '%s','%s' %s)",
            iter_num, class_name, bucketfs_conn, parameters, _variadic_params)
    local _select_query = string.format("SELECT %s%s", _udf_name, _udf_args)
    local query = string.format(_with_clause, _select_query)
    return query
end

---
-- Executes the given set of queries.
--
-- @param queries lua table including queries
-- @param from_index the index where the queries in the lua table start
--
-- @return True if all queries ran successfully.
--
function M._run_queries(queries, from_index)
    local all_success = true
    for i=from_index, #queries do
        local success, result = _G.global_env.pquery(queries[i][1])
        all_success = all_success and success
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
    return all_success
end

---
-- Initiates the Event Loop that handles state transition
--
-- @param schema        name of schema where the Event-Handler udf script is installed
-- @param class_name    name of the class built on the Event-Handler
-- @param bucketfs_conn the name of BucketFS connection
-- @parameters           config parameters of the Event-Handler
--
-- @return TODO
--
function M.init(schema, class_name, bucketfs_conn, parameters)
    local status = "started"
    local iter_num = 0
    local return_column_names = ''

    repeat
        local udf_query = M._prepare_event_handler_udf(
                iter_num,
                return_column_names,
                schema,
                class_name,
                bucketfs_conn,
                parameters)
        local success, result = _G.global_env.pquery(udf_query)
        if not success then
            local error_obj = exa_error.create(
                    "E-AAF-2",
                    "Error occurred in calling Event Handler: " .. result.error_message)
            _G.global_env.error(tostring(error_obj))
        end
        return_column_names = result[1][1]
        status = result[2][1]
        M._run_queries(result, 3)
    until (status == 'completed')

    return status -- TODO return
end


return M;