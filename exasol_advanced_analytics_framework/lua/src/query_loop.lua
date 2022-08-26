---
-- @module query_loop
--
-- This module processes only the state transitions by executing queries returned by the Query Handler
--

local M = {
}
local ExaError = require("ExaError")

function _handle_default_arguments(arguments, meta)
    local query_handler = arguments["query_handler"]
    if query_handler['udf'] == nil then
        local script_schema <const> = meta.script_schema
        query_handler['udf'] = { schema = script_schema, name = 'AAF_QUERY_HANDLER_UDF' }
    end
    return arguments
end

function _generate_temporary_name_prefix(meta)
    local database_name <const> = meta.database_name
    local session_id <const> = tostring(meta.session_id)
    local statement_id <const> = tostring(meta.statement_id)
    local temporary_name <const> = database_name .. '_' .. session_id .. '_' .. statement_id
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
function M.prepare_init_query(arguments, meta)
    local arguments_with_defaults <const> = _handle_default_arguments(arguments, meta)

    local iter_num <const> = 0

    local temporary_output <const> = arguments_with_defaults['temporary_output']
    local temporary_bucketfs_location <const> = temporary_output['bucketfs_location']
    local temporary_bfs_location_conn <const> = temporary_bucketfs_location['connection_name']
    local temporary_bfs_location_directory <const> = temporary_bucketfs_location['directory']
    local temporary_schema_name <const> = temporary_output['schema_name']
    local temporary_name_prefix <const> = _generate_temporary_name_prefix(meta)

    local query_handler <const> = arguments_with_defaults['query_handler']
    local params <const> = query_handler['parameters']
    local python_class <const> = query_handler["class"]
    local python_class_module <const> = python_class['module']
    local python_class_name <const> = python_class['name']

    local udf <const> = query_handler['udf']
    local udf_schema <const> = udf['schema']
    local udf_name <const> = udf['name']

    local full_qualified_udf_name <const> = string.format("%s.%s", udf_schema, udf_name)
    local udf_args <const> = string.format("(%d,'%s','%s','%s','%s','%s','%s','%s')",
            iter_num,
            temporary_bfs_location_conn,
            temporary_bfs_location_directory,
            temporary_name_prefix,
            temporary_schema_name,
            python_class_name,
            python_class_module,
            params)
    local query <const> = string.format("SELECT %s%s", full_qualified_udf_name, udf_args)
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
function M._run_queries(queries, from_index, exa_env)
    local success
    local result
    for i = from_index, #queries do
        local query = queries[i][1]
        if query ~= nil then
            success, result = exa_env.functions.pquery(query)
            if not success then
                -- TODO cleanup after query error
                local error_obj <const> = ExaError:new(
                        "E-AAF-3",
                        "Error occurred while executing the query {{query}}, got error message {{error_message}}",
                        {
                            query = { value = query, description = "Query which failed" },
                            error_message = { value = result.error_message,
                                              description = "Error message received from the database" }
                        },
                        {
                            "Check the query for syntax errors.",
                            "Check if the referenced database objects exist."
                        }
                )
                exa_env.functions.error(tostring(error_obj))
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
function M.run(query_to_query_handler, exa_env)
    local status = false
    local final_result_or_error
    local query_to_create_view
    repeat
        -- call QueryHandlerUDF return queries
        local return_queries = { { query_to_create_view }, { query_to_query_handler } }
        local result = M._run_queries(return_queries, 1, exa_env)

        -- handle QueryHandlerUDF return
        query_to_create_view = result[1][1]
        query_to_query_handler = result[2][1]
        status = result[3][1]
        final_result_or_error = result[4][1]
        M._run_queries(result, 5, exa_env)
    until (status ~= 'CONTINUE')
    if status == 'ERROR' then
        local error_obj = ExaError:new(
                "E-AAF-4",
                "Error occurred during running the QueryHandlerUDF: {{error_message}}",
                { error_message = { value = final_result_or_error,
                                    description = "Error message returned by the QueryHandlerUDF" } }
        )
        exa_env.functions.error(tostring(error_obj))
    end
    return final_result_or_error
end

return M;