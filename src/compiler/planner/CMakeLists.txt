add_subdirectory(join_order)
add_subdirectory(operator)
add_subdirectory(plan)

add_library(neug_compiler_planner
        OBJECT
        join_order_enumerator_context.cpp
        planner.cpp
        query_planner.cpp
        subplans_table.cpp
        gopt_planner.cc
        )
add_dependencies(neug_compiler_planner neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_compiler_planner>
        PARENT_SCOPE)
