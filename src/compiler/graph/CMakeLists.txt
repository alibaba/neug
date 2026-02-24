add_library(neug_graph
        OBJECT
        graph.cpp
        graph_entry.cpp
        on_disk_graph.cpp)
add_dependencies(neug_graph neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_graph>
        PARENT_SCOPE)
