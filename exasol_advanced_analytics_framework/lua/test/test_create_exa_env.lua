local luaunit = require("luaunit")
local exasol_script_tools = require("exasol_script_tools")

test_create_exa_env = {
}

function test_create_exa_env.setUp()
    test_create_exa_env.old_error = _G.error
    _G.query = function(query, param)
        return query, param
    end
    _G.pquery = function(query, param)
        return query, param
    end
    _G.error = function(msg)
        return msg
    end
    test_create_exa_env.exa = {
        meta = {
            script_schema_name = "test"
        }
    }
end

function test_create_exa_env.tearDown()
    _G.query = nil
    _G.pquery = nil
    _G.error = test_create_exa_env.old_error
end

function test_create_exa_env.test_create_exa_env_query()
    local exa_env = exasol_script_tools.create_exa_env(test_create_exa_env.exa)
    local expected_query = "pquery"
    local expected_query_params = "pquery_params"
    local result_query, result_query_params = exa_env.functions.pquery(expected_query, expected_query_params)
    luaunit.assertEquals(result_query, expected_query)
    luaunit.assertEquals(result_query_params, expected_query_params)
end

function test_create_exa_env.test_create_exa_env_pquery()
    local exa_env = exasol_script_tools.create_exa_env(test_create_exa_env.exa)
    local expected_pquery = "query"
    local expected_pquery_params = "query_params"
    local result_pquery, result_pquery_params = exa_env.functions.query(expected_pquery, expected_pquery_params)
    luaunit.assertEquals(result_pquery, expected_pquery)
    luaunit.assertEquals(result_pquery_params, expected_pquery_params)
end

function test_create_exa_env.test_create_exa_env_error()
    local exa_env = exasol_script_tools.create_exa_env(test_create_exa_env.exa)
    local expected_error = "error"
    local result_error = exa_env.functions.error(expected_error)
    luaunit.assertEquals(result_error, expected_error)
end

os.exit(luaunit.LuaUnit.run())
