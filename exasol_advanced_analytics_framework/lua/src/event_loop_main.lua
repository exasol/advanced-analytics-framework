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
-- This is the main function of the Event Loop
--
-- @param json_str	input parameters as json string
--
function main(json_str)
    local args = _parse_arguments(json_str)
    local result = event_loop.init(
            args['schema'],
            args['event_handler_class_name'],
            args['bucketfs_connection'],
            args['parameters']
    )
    return result
end

