CREATE OR REPLACE LUA SCRIPT "AAF_QUERY_LOOP"(json_str) RETURNS TABLE AS
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

{{ bundled_script }}

return main(json_str, exa.meta)

/

