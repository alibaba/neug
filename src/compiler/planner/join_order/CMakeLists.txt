add_library(neug_planner_join_order
        OBJECT
        cardinality_estimator.cpp
        cost_model.cpp
        join_order_util.cpp
        join_plan_solver.cpp
        join_tree.cpp
        join_tree_constructor.cpp)
add_dependencies(neug_planner_join_order neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_planner_join_order>
        PARENT_SCOPE)
