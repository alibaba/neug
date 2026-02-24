add_library(neug_parser_antlr_parser
        OBJECT
        kuzu_cypher_parser.cpp
        parser_error_listener.cpp
        parser_error_strategy.cpp)
add_dependencies(neug_parser_antlr_parser neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_parser_antlr_parser>
        PARENT_SCOPE)
