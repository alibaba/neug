add_library(neug_planner_simple
        OBJECT
        logical_simple.cpp)
add_dependencies(neug_planner_simple neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_planner_simple>
        PARENT_SCOPE)
