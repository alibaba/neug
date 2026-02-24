add_library(
        neug_parser_visitor
        OBJECT
        standalone_call_rewriter.cpp
        statement_read_write_analyzer.cpp)
add_dependencies(neug_parser_visitor neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_parser_visitor>
        PARENT_SCOPE)
