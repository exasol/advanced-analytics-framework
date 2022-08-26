CREATE OR REPLACE LUA SCRIPT "AAF_RUN_QUERY_HANDLER"(json_str) RETURNS TABLE AS
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
package.preload[ "ExaError" ] = function( ... ) local arg = _G.arg;
--- This class provides a uniform way to define errors in a Lua application.
-- @classmod ExaError
local ExaError = {
    VERSION = "2.0.1",
}
ExaError.__index = ExaError

local MessageExpander = require("MessageExpander")

-- Lua 5.1 backward compatibility
-- luacheck: push ignore 122
if not table.unpack then table.unpack = _G.unpack end
-- luacheck: pop

local function expand(message, parameters)
    return MessageExpander:new(message, parameters):expand()
end

--- Convert error to a string representation.
-- Note that `__tostring` is the metamethod called by Lua's global `tostring` function.
-- This allows using the error message in places where Lua expects a string.
-- @return string representation of the error object
function ExaError:__tostring()
    local lines = {}
    if self._code then
        if self._message then
            table.insert(lines, self._code .. ": " .. self:get_message())
        else
            table.insert(lines, self._code)
        end
    else
        if self._message then
            table.insert(lines, self:get_message())
        else
            table.insert(lines, "<Missing error message. This should not happen. Please contact the software maker.>")
        end
    end
    if (self._mitigations ~= nil) and (#self._mitigations > 0) then
        table.insert(lines, "\nMitigations:\n")
        for _, mitigation in ipairs(self._mitigations) do
            table.insert(lines, "* " .. expand(mitigation, self._parameters))
        end
    end
    return table.concat(lines, "\n")
end

--- Concatenate an error object with another object.
-- @return String representing the concatenation.
function ExaError.__concat(left, right)
    return tostring(left) .. tostring(right)
end

--- Create a new instance of an error message.
-- @param code error code
-- @param message error message, optionally with placeholders
-- @param[opt={}] parameters parameter definitions used to replace the placeholders
-- @param[opt={}] mitigations mitigations users can try to solve the error
-- @return created object
function ExaError:new(code, message, parameters, mitigations)
    local instance = setmetatable({}, self)
    instance:_init(code, message, parameters, mitigations)
    return instance
end

function ExaError:_init(code, message, parameters, mitigations)
    self._code = code
    self._message = message
    self._parameters = parameters or {}
    self._mitigations = mitigations or {}
end

--- Add mitigations.
-- @param ... one or more mitigation descriptions
-- @return error message object
function ExaError:add_mitigations(...)
    for _, mitigation in ipairs({...}) do
        table.insert(self._mitigations, mitigation)
    end
    return self
end

--- Add issue ticket mitigation
-- This is a special kind of mitigation which you should use in case of internal software errors that should not happen.
-- For example when a path in the code is reached that should be unreachable if the code is correct.
-- @return error message object
function ExaError:add_ticket_mitigation()
    table.insert(self._mitigations,
        "This is an internal software error. Please report it via the project's ticket tracker.")
    return self
end

--- Get the error code.
-- @return error code
function ExaError:get_code()
    return self._code
end

--- Get the error message.
-- Placeholders in the raw message are replaced by the parameters given when building the error message object.
-- For fault tolerance, this method returns the raw message in case the parameters are missing.
-- @return error message
function ExaError:get_message()
    return expand(self._message, self._parameters)
end

function ExaError:get_raw_message()
    return self._message or ""
end

--- Get parameter definitions.
-- @return parameter defintions
function ExaError:get_parameters()
    return self._parameters
end

--- Get the description of a parameter.
-- @param parameter_name name of the parameter
-- @return parameter description or the string "`<missing parameter description>`"
function ExaError:get_parameter_description(parameter_name)
    return self._parameters[parameter_name].description or "<missing parameter description>"
end

--- Get the mitigations for the error.
-- @return list of mitigations
function ExaError:get_mitigations()
    return table.unpack(self._mitigations)
end

--- Raise the error.
-- Like in Lua's `error` function, you can optionally specify if and from which level down the stack trace
-- is included in the error message.
-- <ul>
-- <li>0: no stack trace</li>
-- <li>1: stack trace starts at the point inside `exaerror` where the error is raised
-- <li>2: stack trace starts at the calling function (default)</li>
-- <li>3+: stack trace starts below the calling function</li>
-- </ul>
-- @param level (optional) level from which down the stack trace will be displayed
-- @raise Lua error for the given error object
function ExaError:raise(level)
    level = (level == nil) and 2 or level
    error(tostring(self), level)
end

--- Raise an error that represents the error object's contents.
-- @param code error code
-- @param message error message, optionally with placeholders
-- @param[opt={}] parameters parameter definitions used to replace the placeholders
-- @param[opt={}] mitigations mitigations users can try to solve the error
-- @see M.create
-- @see M:new
-- @raise Lua error for the given error object
function ExaError.error(code, message, parameters, mitigations)
     ExaError:new(code, message, parameters, mitigations):raise()
end

return ExaError
end
end

do
local _ENV = _ENV
package.preload[ "MessageExpander" ] = function( ... ) local arg = _G.arg;
--- This class provides a parser for messages with named parameters and can expand the message using the parameter
-- values.
-- @classmod MessageExpander
local MessageExpander = {}
MessageExpander.__index = MessageExpander

local FROM_STATE_INDEX = 1
local GUARD_INDEX = 2
local ACTION_INDEX = 3
local TO_STATE_INDEX = 4

--- Create a new instance of a message expander.
-- @param message to be expanded
-- @param parameters parameter definitions
-- @return message expander instance
function MessageExpander:new(message, parameters)
    local instance = setmetatable({}, self)
    instance:_init(message, parameters)
    return instance
end

function MessageExpander:_init(message, parameters)
    self._message = message
    self._parameters = parameters
    self._tokens = {}
    self._last_parameter = {characters = {}, quote = true}
end

local function tokenize(text)
    return string.gmatch(text, ".")
end

--- Expand the message.
-- Note that if no parameter values are supplied, the message will be returned as is, without any replacements.
-- @return expanded message
function MessageExpander:expand()
    if (self._parameters == nil) or (not next(self._parameters)) then
        return self._message
    else
        self:_run()
    end
    return table.concat(self._tokens)
end

function MessageExpander:_run()
    self.state = "TEXT"
    local token_iterator = tokenize(self._message)
    for token in token_iterator do
        self.state = self:_transit(token)
    end
end

function MessageExpander:_transit(token)
    for _, transition in ipairs(MessageExpander._transitions) do
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
    table.insert(self._tokens, token)
end

local function add_open_plus_token(self, token)
    table.insert(self._tokens, "{")
    table.insert(self._tokens, token)
end

local function add_parameter_name(self, token)
    table.insert(self._last_parameter.characters, token)
end

local function set_unquoted(self)
    self._last_parameter.quote = false
end

local function unwrap_parameter_value(parameter)
    if parameter ~= nil and type(parameter) == "table" then
        return parameter.value
    else
        return parameter
    end
end

local function insert_parameter_value_into_token_list(self, value)
    if value == nil then
        table.insert(self._tokens, "<missing value>")
    else
        local type = type(value)
        if (type == "string") and (self._last_parameter.quote) then
            table.insert(self._tokens, "'")
            table.insert(self._tokens, value)
            table.insert(self._tokens, "'")
        elseif type == "boolean" then
            table.insert(self._tokens, tostring(value))
        else
            table.insert(self._tokens, value)
        end
    end
end

local function replace_parameter(self)
    local parameter_name = table.concat(self._last_parameter.characters)
    local value = unwrap_parameter_value(self._parameters[parameter_name])
    insert_parameter_value_into_token_list(self, value)
    self._last_parameter.characters = {}
    self._last_parameter.quote = true
end

local function replace_and_add(self, token)
    replace_parameter(self)
    add_token(self, token)
end

local function do_nothing() end

MessageExpander._transitions = {
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

return MessageExpander
end
end

do
local _ENV = _ENV
package.preload[ "exasol_script_tools" ] = function( ... ) local arg = _G.arg;
---
-- @module exa_script_tools
--
-- This module contains utilities for Lua Scripts in Exasol
--

local M = {
}

local ExaError = require("ExaError")
local json = require("cjson")

---
-- Extend `exa-object` with the global functions available in Lua Scripts.
-- See Exasol Scripting: https://docs.exasol.com/db/latest/database_concepts/scripting/db_interaction.htm
-- We provide this function such that implementers of a main function can encapsulate the global objects
-- and properly inject them into modules.
--
-- @param exa exa-object available inside of Lua Scripts
--
-- @return lua table including meta and functions
--
function M.create_exa_env(exa)
    local exa_env = {
        meta = exa.meta,
        -- We put the global functions into a subtable, such that we can replace the subtable with a mock
        functions = {
            pquery = pquery,
            query = query,
            error = error
        }
    }
    return exa_env
end

---
-- Parse a given arguments in JSON string format.
--
-- @param json_str input parameters as JSON string
--
-- @return Lua table containing the parameters
--
function M.parse_arguments(json_str, exa_env)
    local success, args = pcall(json.decode, json_str)
    if not success then
        local error_obj = ExaError:new(
                "E-AAF-1",
                "Arguments could not be converted from JSON object to Lua table: {{raw_json}}",
                { raw_json = { value = json_str, description = "raw JSON object" } },
                { "Check syntax of the input string JSON is correct" }
        )
        exa_env.functions.error(tostring(error_obj))
    end
    return args
end

---
-- Encapsulates the result of a QueryHandler such that it can be returns from a Exasol Lua Script.
--
-- @param result A string containing the result of the QueryHandler
--
-- @return A tuple of a table with a single row and one column and the SQL column definition for it
--
function M.wrap_result(result)
    local return_result = { { result } }
    return return_result, "result_column VARCHAR(2000000)"
end

return M
end
end

do
local _ENV = _ENV
package.preload[ "query_handler_runner" ] = function( ... ) local arg = _G.arg;
---
-- @module query_handler_runner
--
-- This modules includes the run function of the Query Loop
--

M = {
    _query_loop = require("query_loop"),
    _exasol_script_tools = require("exasol_script_tools")
}

function M.run(json_str, exa)
    local exa_env = M._exasol_script_tools.create_exa_env(exa)
    local args = M._exasol_script_tools.parse_arguments(json_str)
    local init_query = M._query_loop.prepare_init_query(args, exa_env.meta)
    local result = M._query_loop.run(init_query, exa_env)
    return M._exasol_script_tools.wrap_result(result)
end

return M
end
end

do
local _ENV = _ENV
package.preload[ "query_loop" ] = function( ... ) local arg = _G.arg;
---
-- @module query_loop
--
-- This module processes only the state transitions by executing queries returned by the Query Handler
--

local M = {
}
local ExaError = require("ExaError")

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
function M._run_queries(queries, from_index, exa_env)
    local success
    local result
    for i = from_index, #queries do
        local query = queries[i][1]
        if query ~= nil then
            success, result = exa_env.functions.pquery(query)
            if not success then
                -- TODO cleanup after query error
                local error_obj = ExaError:new(
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
end
end

---
-- @module query_loop_main
--
-- This script contains the main function of the Query Loop.
--

query_handler_runner = require("query_handler_runner")
---
-- This is the main function of the Query Loop.
--
-- @param json_str	input parameters as JSON string
-- @param exa	the database context (`exa`) of the Lua script
--
function query_handler_runner_main(json_str, exa)
    return query_handler_runner.run(json_str, exa)
end



return query_handler_runner_main(json_str, exa)

/
