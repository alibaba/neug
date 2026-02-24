add_library(
        neug_binder_query
        OBJECT
        bound_insert_clause.cpp
        bound_delete_clause.cpp
        bound_merge_clause.cpp
        bound_set_clause.cpp
        query_graph.cpp
        query_graph_label_analyzer.cpp)
add_dependencies(neug_binder_query neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_binder_query>
        PARENT_SCOPE)
