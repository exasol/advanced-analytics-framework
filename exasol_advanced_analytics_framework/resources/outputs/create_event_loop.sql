CREATE OR REPLACE LUA SCRIPT "AAF_EVENT_LOOP"(json_str) RETURNS TABLE AS
    table.insert(_G.package.searchers,
        function (module_name)
            local loader = package.preload[module_name]
            if not loader then
                error("Module " .. module_name .. " not found in package.preload.")
            else
                return loader
            end
        end
    )

do
local _ENV = _ENV
package.preload[ "event_loop" ] = function( ... ) local arg = _G.arg;
---
-- @module event_loop
--
-- This module processes only the state transitions by executing queries returned by the Event Handler
--

local M = {
    string_to_boolean = { ["True"]=true, ["False"]=false }
}
local exa_error = require("exaerror")

_G.global_env = {
    pquery = pquery,
    error = error
}

---
-- Executes the given set of queries.
--
-- @param   queries lua table including queries
-- @param   from_index the index where the queries in the lua table start
--
-- @return  the result of the latest query
--
function M._run_queries(queries, from_index)
    for i=from_index, #queries do
        if queries[i][1] ~= nil then
            success, result = _G.global_env.pquery(queries[i][1])
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
    end
    return result
end

---
-- Initiates the Event Loop that handles state transition
--
-- @param query string that calls the event handler
--
function M.init(query_to_event_handler)
    local is_finished = false
    local final_result = nil
    local query_to_create_view = nil
    repeat
        -- call EventHandlerUDF return queries
        local return_queries = {{query_to_create_view}, {query_to_event_handler}}
        local result = M._run_queries(return_queries, 1)

        -- handle EventHandlerUDF return
        query_to_create_view = result[1][1]
        query_to_event_handler = result[2][1]
        is_finished = M.string_to_boolean[result[3][1]]
        final_result = result[4][1]
        M._run_queries(result, 5)
    until (is_finished)

    return final_result
end


return M;
end
end

do
local _ENV = _ENV
package.preload[ "exaerror" ] = function( ... ) local arg = _G.arg;
---
-- This module provides a uniform way to define errors in a Lua application.
--
-- @module M
--
local M = {
    VERSION = "1.2.0",
}

local msgexpander = require("message_expander")

-- Lua 5.1 backward compatibility
_G.unpack = table.unpack or _G.unpack

local function expand(message, parameters)
    return msgexpander:new({message =message, parameters = parameters}):expand()
end

---
-- Convert error to a string representation.
-- <p>
-- Note that <code>__tostring</code> is the metamethod called by Lua's global <code>tostring</code> function.
-- This allows using the error message in places where Lua expects a string.
-- </p>
--
-- @return string representation of the error object
--
function M:__tostring()
    local lines = {}
    if self.code then
        if self.message then
            table.insert(lines, self.code .. ": " .. self:get_message())
        else
            table.insert(lines, self.code)
        end
    else
        if self.message then
            table.insert(lines, self:get_message())
        else
            table.insert(lines, "<Missing error message. This should not happen. Please contact the software maker.>")
        end
    end
    if (self.mitigations ~= nil) and (#self.mitigations > 0) then
        table.insert(lines, "\nMitigations:\n")
        for _, mitigation in ipairs(self.mitigations) do
            table.insert(lines, "* " .. expand(mitigation, self.parameters))
        end
    end
    return table.concat(lines, "\n")
end

---
-- Concatenate an error object with another object.
--
-- @return String representing the concatenation.
--
function M.__concat(left, right)
    return tostring(left) .. tostring(right)
end

---
-- Create a new instance of an error message.
--
-- @param object pre-initialized object to be used for the instance
--        (optional) a new object is created if you don't provide one
--
-- @return created object
--
function M:new(object)
    object = object or {}
    self.__index = self
    setmetatable(object, self)
    return object
end

---
-- Factory method for a new error message.
--
-- @param code error code
-- @param message message body
-- @param parameters parameters to replace the placeholders in the message (if any)
--
-- @return created object
--
function M.create(code, message, parameters)
    return M:new({code = code, message = message, parameters = parameters, mitigations = {}})
end

---
-- Add mitigations.
--
-- @param ... one or more mitigation descriptions
--
-- @return error message object
--
function M:add_mitigations(...)
    for _, mitigation in ipairs({...}) do
        table.insert(self.mitigations, mitigation)
    end
    return self
end

---
-- Add issue ticket mitigation
-- <p>
-- This is a special kind of mitigation which you should use in case of internal software errors that should not happen.
-- For example when a path in the code is reached that should be unreachable if the code is correct.
-- </p>
--
-- @return error message object
--
function M:add_ticket_mitigation()
    table.insert(self.mitigations,
        "This is an internal software error. Please report it via the project's ticket tracker.")
    return self
end


---
-- Get the error code.
--
-- @return error code
--
function M:get_code()
    return self.code
end

---
-- Get the error message.
-- <p>
-- This method supports Lua's standard string interpolation used in <code>string.format</code>.
-- Placeholders in the raw message are replaced by the parameters given when building the error message object.
-- </p>
-- <p>
-- For fault tolerance, this method returns the raw message in case the parameters are missing.
-- </p>
--
-- @return error message
--
function M:get_message()
    return expand(self.message, self.parameters)
end

function M:get_raw_message()
    return self.message or ""
end

---
-- Get parameter definitions.
--
function M:get_parameters()
    return self.parameters
end

---
-- Get the description of a parameter.
--
-- @parameter parameter_name name of the parameter
--
-- @return parameter description or the string "<code><missing parameter description></code>"
--
function M:get_parameter_description(parameter_name)
    return self.parameters[parameter_name].description or "<missing parameter description>"
end

---
-- Get the mitigations for the error.
--
-- @return list of mitigations
--
function M:get_mitigations()
    return unpack(self.mitigations)
end

---
-- Raise the error.
-- <p>
-- Like in Lua's <code>error</code> function, you can optionally specify if and from which level down the stack trace
-- is included in the error message.
-- </p>
-- <ul>
-- <li>0: no stack trace</li>
-- <li>1: stack trace starts at the point inside <code>exaerror</code> where the error is raised
-- <li>2: stack trace starts at the calling function (default)</li>
-- <li>3+: stack trace starts below the calling function</li>
-- </ul>
--
-- @parameter level (optional) level from which down the stack trace will be displayed
--
-- @raise Lua error for the given error object
--
function M:raise(level)
    level = (level == nil) and 2 or level
    error(tostring(self), level)
end

---
-- Raise an error that represents the error object's contents.
-- <p>
-- This function supports two calling styles. Either like <code>exaerror:create</code> with a flat list of parameters.
-- Or like <code>exaerror:new</code> with a table to preinitialize the error object.
-- </p>
-- <p>The first parameter decides on the calling convention. If it is a table, <code>exaerror:new</code is used.
--
-- @see M.create
-- @see M:new
--
-- @raise Lua error for the given error object
--
function M.error(arg1, ...)
    if type(arg1) == "table" then
        M:new(arg1):raise()
    else
        M.create(arg1, ...):raise()
    end
end

return M
end
end

do
local _ENV = _ENV
package.preload[ "message_expander" ] = function( ... ) local arg = _G.arg;
---
-- This module provides a parser for messages with named parameters and can expand the message using the parameter
-- values.
--
-- @module M
--
local M = {}

local FROM_STATE_INDEX = 1
local GUARD_INDEX = 2
local ACTION_INDEX = 3
local TO_STATE_INDEX = 4

---
-- Create a new instance of a message expander.
--
-- @param object pre-initialized object to be used for the instance
--        (optional) a new object is created if you don't provide one
--
-- @return created object
--
function M:new(object)
    object = object or {}
    self.__index = self
    setmetatable(object, self)
    self.tokens_ = {}
    self.last_parameter_ = {characters = {}, quote = true}
    return object
end

---
-- Create new instance of a message expander.
--
-- @parameter message to be expanded
-- @parameter ... values used to replace the placeholders
--
function M.create(message, ...)
    return M:new({
        message = message,
        parameters = {...},
    })
end

local function tokenize(text)
    return string.gmatch(text, ".")
end

---
-- Expand the message.
-- <p>
-- Note that if no parameter values are supplied, the message will be returned as is, without any replacements.
-- </p>
--
-- @return expanded message
--
function M:expand()
    if (self.parameters == nil) or (not next(self.parameters)) then
        return self.message
    else
        self:run_()
    end
    return table.concat(self.tokens_)
end

function M:run_()
    self.state = "TEXT"
    local token_iterator = tokenize(self.message)
    for token in token_iterator do
        self.state = self:transit_(token)
    end
end

function M:transit_(token)
    for _, transition in ipairs(M.transitions_) do
        local from_state = transition[FROM_STATE_INDEX]
        local guard = transition[GUARD_INDEX]
        if(from_state == self.state and guard(token)) then
            local action = transition[ACTION_INDEX]
            action(self, token)
            local to_state = transition[TO_STATE_INDEX]
            return to_state
        end
    end
end

local function is_any()
    return true
end

local function is_opening_bracket(token)
    return token == "{"
end

local function is_closing_bracket(token)
    return token == "}"
end

-- We are intentionally not using the symbol itself here for compatibility reasons.
-- See https://github.com/exasol/error-reporting-lua/issues/15 for details.
local function is_pipe(token)
    return token == string.char(124)
end

local function is_u(token)
    return token == "u"
end

local function is_not_bracket(token)
    return not is_opening_bracket(token) and not is_closing_bracket(token)
end

local function add_token(self, token)
    table.insert(self.tokens_, token)
end

local function add_open_plus_token(self, token)
    table.insert(self.tokens_, "{")
    table.insert(self.tokens_, token)
end

local function add_parameter_name(self, token)
    table.insert(self.last_parameter_.characters, token)
end

local function set_unquoted(self)
    self.last_parameter_.quote = false
end

local function unwrap_parameter_value(parameter)
    if parameter == nil then
        return "missing value"
    else
        local type = type(parameter)
        if (type == "table") then
            return parameter.value
        else
            return parameter
        end
    end
end

local function replace_parameter(self)
    local parameter_name = table.concat(self.last_parameter_.characters)
    local value = unwrap_parameter_value(self.parameters[parameter_name])
    local type = type(value)
    if (type == "string") and (self.last_parameter_.quote) then
        table.insert(self.tokens_, "'")
        table.insert(self.tokens_, value)
        table.insert(self.tokens_, "'")
    else
        table.insert(self.tokens_, value)
    end
    self.last_parameter_.characters = {}
    self.last_parameter_.quote = true
end

local function replace_and_add(self, token)
    replace_parameter(self)
    add_token(self, token)
end

local function do_nothing() end

M.transitions_ = {
    {"TEXT"     , is_not_bracket    , add_token          , "TEXT"     },
    {"TEXT"     , is_opening_bracket, do_nothing         , "OPEN_1"   },
    {"OPEN_1"   , is_opening_bracket, do_nothing         , "PARAMETER"},
    {"OPEN_1"   , is_any            , add_open_plus_token, "TEXT"     },
    {"PARAMETER", is_closing_bracket, do_nothing         , "CLOSE_1"  },
    {"PARAMETER", is_pipe           , do_nothing         , "SWITCH"   },
    {"PARAMETER", is_any            , add_parameter_name , "PARAMETER"},
    {"SWITCH"   , is_closing_bracket, do_nothing         , "CLOSE_1"  },
    {"SWITCH"   , is_u              , set_unquoted       , "SWITCH"   },
    {"SWITCH"   , is_any            , do_nothing         , "SWITCH"   },
    {"CLOSE_1"  , is_closing_bracket, replace_parameter  , "TEXT"     },
    {"CLOSE_1"  , is_any            , replace_and_add    , "TEXT"     }
}

return M
end
end

---
-- @module event_loop_main
--
-- This script includes the main function of the Event Loop
--


local exaerror = require("exaerror")
local event_loop = require("event_loop")
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
    local event_handler = args["event_handler"]
    if event_handler['udf'] == nil then
        local script_schema = meta.script_schema
        event_handler['udf'] = { schema = script_schema, name = 'AAF_EVENT_HANDLER_UDF' }
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
-- Prepare the initial query that initiates the Event Loop and calls Event Handler
--
-- @param args      lua table including parameters
-- @param udf_name  name of the udf that calls event handler
--
-- @return query string that calls the event handler
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

    local event_handler = args['event_handler']
    local params = event_handler['parameters']
    local python_class = event_handler["class"]
    local python_class_module = python_class['module']
    local python_class_name = python_class['name']

    local udf = event_handler['udf']
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
-- This is the main function of the Event Loop
--
-- @param json_str	input parameters as json string
--
function main(json_str, meta)
    local args = _parse_arguments(json_str)
    local init_query = _prepare_init_query(args, meta)
    local result = event_loop.init(init_query)

    local return_result = {}
    return_result[#return_result + 1] = { result }
    return return_result, "result_column varchar(1000000)"
end



return main(json_str, exa.meta)

/
