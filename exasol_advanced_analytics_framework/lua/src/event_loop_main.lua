---
-- @module event_loop_main
--
-- This script includes the main function of the Event Loop
--


local exaerror = require("exaerror")
local event_loop = require("event_loop")
local json = require('cjson')

_G.global_env = {
    error = error
}


---
-- Parse a given arguments in json string format.
--
-- @param json_str input parameters as json string
--
-- @return lua table including parameters
--
function _parse_arguments(json_str)
    local success, args =  pcall(json.decode, json_str)
    if not success then
		local error_obj = exaerror.create(
                "E-AAF-1",
                "It could not be converted to json object"
        ):add_mitigations("Check syntax of the input string json is correct")
		_G.global_env.error(tostring(error_obj))
	end
    return args
end

---
-- Prepare the initial query that initiates the Event Loop and calls Event Handler
--
-- @param args      lua table including parameters
-- @param udf_name  name of the udf that calls event handler
--
-- @return query string that calls the event handler
--
function _prepare_init_query(args)
    local iter_num = 0
    local udf_name = string.upper("AAF_EVENT_HANDLER_UDF")
    local schema = args['schema']
    local bfs_conn = args['bucketfs_connection']
    local cls_module = args['event_handler_module']
    local cls_name = args['event_handler_class']
    local params = args['event_handler_parameters']

    local _udf_name = string.format("%s.%s", schema, udf_name)
    local _udf_args = string.format("(%d,'%s','%s','%s','%s')",
            iter_num, bfs_conn, cls_name, cls_module, params)
    local query = string.format("SELECT %s%s", _udf_name, _udf_args)
    return query
end

---
-- This is the main function of the Event Loop
--
-- @param json_str	input parameters as json string
--
function main(json_str)
    local args = _parse_arguments(json_str)
    local init_query = _prepare_init_query(args)

    local result = event_loop.init(init_query)
    return result
end

