package = "advanced-analytics-framework"
version = "0.1.0-1"
source = {
   url = "git://github.com/exasol/advanced-analytics-framework"
}
description = {
   detailed = "Framework for building complex data analysis algorithms with Exasol",
   homepage = "https://github.com/exasol/advanced-analytics-framework",
   license = "MIT"
}
dependencies = {
    "lua >= 5.1",
    "amalg >= 0.8-1",
    "lua-cjson >= 2.1.0.6-1",
    "luaunit == 3.3.-1",
    "mockagne >= 1.0-2",
    "exaerror <= 1.2.1-1",
    "luacheck >= 0.25.0-1",
    "luacov >= 0.15.0-1",
    "luacov-coveralls >= 0.2.3-1",
    "inspect >= 3.1.3-0"
}
build = {
   type = "builtin",
   modules = {
      ["exasol_advanced_analytics_framework.lua.src.exasol_script_tools"] = "exasol_advanced_analytics_framework/lua/src/exasol_script_tools.lua",
      ["exasol_advanced_analytics_framework.lua.src.query_loop"] = "exasol_advanced_analytics_framework/lua/src/query_loop.lua",
      ["exasol_advanced_analytics_framework.lua.src.query_handler_runner_main"] = "exasol_advanced_analytics_framework/lua/src/query_handler_runner_main.lua",
      ["exasol_advanced_analytics_framework.lua.src.query_handler_runner"] = "exasol_advanced_analytics_framework/lua/src/query_handler_runner.lua"
   },
   copy_directories = {
      "doc",
      "tests"
   }
}
