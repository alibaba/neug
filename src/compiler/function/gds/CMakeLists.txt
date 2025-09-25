add_library(gds_function
        STATIC
        bfs_graph.cpp
        gds_frontier.cpp
        gds.cpp
        output_writer.cpp
        gds_state.cpp
        rec_joins.cpp
        gds_utils.cpp
        frontier_morsel.cpp
        gds_task.cpp
        )
add_dependencies(gds_function neug_plan_proto)

target_include_directories(gds_function PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../include)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:gds_function>
        PARENT_SCOPE)
