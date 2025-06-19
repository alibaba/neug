file(GLOB_RECURSE neug_MAIN_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../utils)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../compiler)
add_library(neug_main SHARED ${neug_MAIN_SRC_FILES})
target_link_libraries(neug_main neug_utils neug_graph_db neug_planner neug_compiler_shared)
install_without_export_neug_target(neug_main)




