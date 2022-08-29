---
-- @module query_loop
--
-- This module processes only the state transitions by executing queries returned by the Query Handler
--

local M = {
}
local ExaError = require("ExaError")

local function _handle_default_arguments(arguments, meta)
    local query_handler = arguments["query_handler"]
    if query_handler['udf'] == nil then
        local script_schema <const> = meta.script_schema
        query_handler['udf'] = { schema = script_schema, name = 'AAF_QUERY_HANDLER_UDF' }
    end
    return arguments
end

local function _generate_temporary_name_prefix(meta)
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

local FIRST_COLUMN_INDEX <const> = 1

local function _handle_query_error(query, result, exa_env)
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
        local query = queries[i][FIRST_COLUMN_INDEX]
        if query ~= nil then
            success, result = exa_env.functions.pquery(query)
            if not success then
                _handle_query_error(query, result, exa_env)
            end
        end
    end
    return result
end

local function _call_query_handler(input_view_query, call_query, exa_env)
    local start_row_index <const> = 1
    local call_queries <const> = {
        { input_view_query },
        { call_query }
    }
    local result <const> = M._run_queries(
            call_queries,
            start_row_index,
            exa_env)
    return result
end

local function _handle_query_handler_call_result(call_result, exa_env)
    local input_view_query_row_index <const> = 1
    local call_query_row_index <const> = 2
    local status_row_index <const> = 3
    local final_result_or_error_row_index <const> = 4
    local returned_queries_start_row_index <const> = 5
    local input_view_query <const> = call_result[input_view_query_row_index][FIRST_COLUMN_INDEX]
    local call_query <const> = call_result[call_query_row_index][FIRST_COLUMN_INDEX]
    local status <const> = call_result[status_row_index][FIRST_COLUMN_INDEX]
    local final_result_or_error <const> = call_result[final_result_or_error_row_index][FIRST_COLUMN_INDEX]
    M._run_queries(call_result, returned_queries_start_row_index, exa_env)
    local state <const> = {
        input_view_query = input_view_query,
        call_query = call_query,
        status = status,
        final_result_or_error = final_result_or_error
    }
    return state
end

local function _run_query_handler_iteration(old_state, exa_env)
    local call_result <const> = _call_query_handler(
            old_state.input_view_query,
            old_state.call_query,
            exa_env)
    local new_state <const> = _handle_query_handler_call_result(call_result, exa_env)
    return new_state
end

local function _handle_query_handler_error(new_state, old_state, exa_env)
    local input_view = old_state.input_view_query
    if old_state.input_view_query == nil then
        input_view = "Not used"
    end
    local error_obj <const> = ExaError:new(
            "E-AAF-4",
            [[Error occurred while calling the query handler.
Call-Query: {{call_query}}
Input-View: {{input_view}}
Error Message: {{error_message}}]],
            {
                call_query = { value = old_state.call_query,
                               description = "Query which was used to call the QueryHandler" },
                input_view = { value = input_view,
                               description = "View used as input for the Call-Query" },
                error_message = { value = new_state.final_result_or_error,
                                  description = "Error message returned by the QueryHandlerUDF" } }
    )
    exa_env.functions.error(tostring(error_obj))
end

---
-- Initiates the Query Loop that handles state transition
--
-- @param query string that calls the query handler
--
function M.run(init_call_query, exa_env)
    local new_state = {
        input_view_query = nil,
        call_query = init_call_query
    }
    local old_state = new_state
    repeat
        old_state = new_state
        new_state = _run_query_handler_iteration(old_state, exa_env)
    until (new_state.status ~= 'CONTINUE')
    if new_state.status == 'ERROR' then
        _handle_query_handler_error(new_state, old_state, exa_env)
    end
    return new_state.final_result_or_error
end

return M;