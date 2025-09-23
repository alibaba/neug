add_library(neug_planner_persistent
        OBJECT
        logical_copy_from.cpp
        logical_copy_to.cpp
        logical_insert.cpp
        logical_delete.cpp
        logical_merge.cpp
        logical_set.cpp
)
add_dependencies(neug_planner_persistent neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_planner_persistent>
        PARENT_SCOPE)
