add_library(neug_planner_scan
        OBJECT
        logical_expressions_scan.cpp
        logical_index_look_up.cpp
        logical_scan_node_table.cpp)
add_dependencies(neug_planner_scan neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_planner_scan>
        PARENT_SCOPE)
