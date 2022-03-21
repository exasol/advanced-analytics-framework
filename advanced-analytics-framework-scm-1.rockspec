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
    "luaunit >= 3.4.-1",
    "mockagne >= 1.0-2",
    "exaerror >= 1.1.0-1",
}
build = {
   type = "builtin",
   modules = {
      ["exasol_advanced_analytics_framework.lua.src.event_loop"] = "exasol_advanced_analytics_framework/lua/src/event_loop.lua",
      ["exasol_advanced_analytics_framework.lua.src.main"] = "exasol_advanced_analytics_framework/lua/src/main.lua"
   },
   copy_directories = {
      "doc",
      "tests"
   }
}
