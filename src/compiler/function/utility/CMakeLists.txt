add_library(neug_utility_function
        OBJECT
        coalesce.cpp
        md5.cpp
        constant_or_null.cpp
        count_if.cpp
        error.cpp
        nullif.cpp
        typeof.cpp)
add_dependencies(neug_utility_function neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_utility_function>
        PARENT_SCOPE)
