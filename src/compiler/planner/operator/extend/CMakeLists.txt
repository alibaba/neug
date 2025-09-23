add_library(neug_planner_extend
        OBJECT
        base_logical_extend.cpp
        logical_extend.cpp
        logical_recursive_extend.cpp)
add_dependencies(neug_planner_extend neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_planner_extend>
        PARENT_SCOPE)