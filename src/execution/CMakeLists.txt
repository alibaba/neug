# There seems to be a circle dependency between graph_db, runtime_common and runtime_execute
# So currently we just add all the source files to the graph_db library
file(GLOB_RECURSE EXECUTION_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/common/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/execute/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/utils/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/extension/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/expression/*.cc")

add_library(neug_execution OBJECT ${EXECUTION_SRC_FILES})
add_dependencies(neug_execution neug_plan_proto)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_execution> PARENT_SCOPE)
target_include_directories(neug_execution PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)
