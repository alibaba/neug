add_library(neug_parser_expression
        OBJECT
        parsed_case_expression.cpp
        parsed_expression.cpp
        parsed_expression_visitor.cpp
        parsed_function_expression.cpp
        parsed_property_expression.cpp
        parsed_variable_expression.cpp)
add_dependencies(neug_parser_expression neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_parser_expression>
        PARENT_SCOPE)
