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

---
-- This is the main function of the Query Loop
--
-- @param json_str	input parameters as json string
--
function main(json_str, meta)
    local args = _parse_arguments(json_str)
    local init_query = query_loop.prepare_init_query(args, meta)
    local result = query_loop.init(init_query)

    local return_result = {}
    return_result[#return_result + 1] = { result }
    return return_result, "result_column varchar(1000000)"
end

