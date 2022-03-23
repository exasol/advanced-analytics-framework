---
-- @module event_loop_main
--
-- This script includes the main function of the Event Loop
--

local M = {}
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
function M._parse_arguments(json_str)
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
-- @param args  lua table including parameters
--
-- @return query string that calls the event handler
--
function M._prepare_init_query(args)
    local udf_name = args['udf_name']
    local params = args['parameters']
    local schema = args['schema']
    local bfs_conn = args['bucketfs_connection']

    local _udf_name = string.format("%s.%s", schema, udf_name)
    local _udf_args = string.format("('%s','%s')", params, bfs_conn)
    local query = string.format("SELECT %s%s", _udf_name, _udf_args)
    return query
end

---
-- This is the main function of the Event Loop
--
-- @param json_str	input parameters as json string
--
function M.main(json_str)
    local args = M._parse_arguments(json_str)
    local init_query = M._prepare_init_query(args)
    local result = event_loop.init(init_query)
    return result
end

return M;