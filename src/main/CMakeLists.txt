file(GLOB_RECURSE NEXG_MAIN_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../utils)
add_library(nexg_main SHARED ${NEXG_MAIN_SRC_FILES})
target_link_libraries(nexg_main nexg_utils nexg_graph_db nexg_planner)
install_nexg_target(nexg_main)




