local test_module_1 = require("test_module_1")

function main()
    local return_result = test_module_1.run()
    return return_result
end

print(main())