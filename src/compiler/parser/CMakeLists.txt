add_subdirectory(antlr_parser)
add_subdirectory(expression)
add_subdirectory(transform)
add_subdirectory(visitor)

add_library(neug_parser
        OBJECT
        create_macro.cpp
        parser.cpp
        parsed_statement_visitor.cpp
        transformer.cpp)

add_dependencies(neug_parser neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_parser>
        PARENT_SCOPE)
