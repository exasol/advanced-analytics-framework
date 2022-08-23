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

function _handle_default_arguments(args, meta)
    local query_handler = args["query_handler"]
    if query_handler['udf'] == nil then
        local script_schema = meta.script_schema
        query_handler['udf'] = { schema = script_schema, name = 'AAF_QUERY_HANDLER_UDF' }
    end
    return args
end

function _generate_temporary_name_prefix(meta)
    local database_name = meta.database_name
    local session_id = tostring(meta.session_id)
    local statement_id = tostring(meta.statement_id)
    local temporary_name = database_name .. '_' .. session_id .. '_' .. statement_id
    return temporary_name
end

---
-- Prepare the initial query that initiates the Query Loop and calls Query Handler
--
-- @param args      lua table including parameters
-- @param udf_name  name of the udf that calls query handler
--
-- @return query string that calls the query handler
--
function M.prepare_init_query(args, meta)
    args = _handle_default_arguments(args, meta)

    local iter_num = 0

    local temporary_output = args['temporary_output']
    local temporary_bucketfs_location = temporary_output['bucketfs_location']
    local temporary_bfs_location_conn = temporary_bucketfs_location['connection_name']
    local temporary_bfs_location_directory = temporary_bucketfs_location['directory']
    local temporary_schema_name = temporary_output['schema_name']
    local temporary_name_prefix = _generate_temporary_name_prefix(meta)

    local query_handler = args['query_handler']
    local params = query_handler['parameters']
    local python_class = query_handler["class"]
    local python_class_module = python_class['module']
    local python_class_name = python_class['name']

    local udf = query_handler['udf']
    local udf_schema = udf['schema']
    local udf_name = udf['name']

    local full_qualified_udf_name = string.format("%s.%s", udf_schema, udf_name)
    local udf_args = string.format("(%d,'%s','%s','%s','%s','%s','%s','%s')",
            iter_num,
            temporary_bfs_location_conn,
            temporary_bfs_location_directory,
            temporary_name_prefix,
            temporary_schema_name,
            python_class_name,
            python_class_module,
            params)
    local query = string.format("SELECT %s%s", full_qualified_udf_name, udf_args)
    return query
end


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