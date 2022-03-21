---
-- @module event_loop_main
--
-- This scrip includes the main function of the Event Loop
--

local M = {}

local exaerror = require("exaerror")
local event_loop = require("event_loop")
local json = require('cjson')

function M._prepare_init_query(args)
    -- TODO
    return nil
end

function M.main(exa, json_str)
    local success, args =  pcall(json.decode, json_str)
    if not success then
		local error_obj = exaerror.create() -- TODO
		error(tostring(error_obj))
	end

    local init_query = M._prepare_init_query(args)
    local result = event_loop.init(init_query)
    return result
end

return M;