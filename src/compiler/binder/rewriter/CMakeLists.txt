add_library(
        neug_binder_rewriter
        OBJECT
        match_clause_pattern_label_rewriter.cpp
        normalized_query_part_match_rewriter.cpp
        with_clause_projection_rewriter.cpp)
add_dependencies(neug_binder_rewriter neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_binder_rewriter>
        PARENT_SCOPE)
