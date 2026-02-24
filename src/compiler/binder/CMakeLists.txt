add_subdirectory(bind)
add_subdirectory(bind_expression)
add_subdirectory(ddl)
add_subdirectory(expression)
add_subdirectory(query)
add_subdirectory(rewriter)
add_subdirectory(visitor)

add_library(neug_binder
        OBJECT
        binder.cpp
        binder_scope.cpp
        bound_statement_result.cpp
        bound_scan_source.cpp
        bound_statement_rewriter.cpp
        bound_statement_visitor.cpp
        expression_binder.cpp
        expression_visitor.cpp
        expression_evaluator.cpp
        expression_evaluator_utils.cpp
        expression_mapper.cpp
        function_evaluator.cpp
        literal_evaluator.cpp)
add_dependencies(neug_binder neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_binder>
        PARENT_SCOPE)
