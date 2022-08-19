---
-- @module query_loop_main
--
-- This script includes the main function of the Query Loop
--


local exaerror = require("exaerror")
local query_loop = require("query_loop")
local json = require('cjson')


---
-- Parse a given arguments in json string format.
--
-- @param json_str input parameters as json string
--
-- @return lua table including parameters
--
function _parse_arguments(json_str)
    local success, args = pcall(json.decode, json_str)
    if not success then
        local error_obj = exaerror.create(
                "E-AAF-1",
                "It could not be converted to json object"
        )                         :add_mitigations("Check syntax of the input string json is correct")
        _G.global_env.error(tostring(error_obj))
    end
    return args
end

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
function _prepare_init_query(args, meta)
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
-- This is the main function of the Query Loop
--
-- @param json_str	input parameters as json string
--
function main(json_str, meta)
    local args = _parse_arguments(json_str)
    local init_query = _prepare_init_query(args, meta)
    local result = query_loop.init(init_query)

    local return_result = {}
    return_result[#return_result + 1] = { result }
    return return_result, "result_column varchar(1000000)"
end

