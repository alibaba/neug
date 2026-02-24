add_library(neug_planner_factorization
        OBJECT
        flatten_resolver.cpp
        sink_util.cpp)
add_dependencies(neug_planner_factorization neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_planner_factorization>
        PARENT_SCOPE)
