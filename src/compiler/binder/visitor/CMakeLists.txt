add_library(
        neug_binder_visitor
        OBJECT
        confidential_statement_analyzer.cpp
        default_type_solver.cpp
        property_collector.cpp)
add_dependencies(neug_binder_visitor neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_binder_visitor>
        PARENT_SCOPE)
