add_library(neug_function_aggregate
        OBJECT
        count.cpp
        count_star.cpp
        collect.cpp
        min_max.cpp
        sum.cpp
        avg.cpp)
add_dependencies(neug_function_aggregate neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_function_aggregate>
        PARENT_SCOPE)
