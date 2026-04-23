add_library(gds_function
        STATIC
        rec_joins.cpp
        project_graph_function.cpp
        gds_algo_function.cpp)
add_dependencies(gds_function neug_plan_proto)

target_include_directories(gds_function PUBLIC
        ${CMAKE_CURRENT_SOURCE_DIR}/../include)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:gds_function>
        PARENT_SCOPE)
