
file(GLOB_RECURSE NEXG_MAIN_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(nexg_main SHARED ${NEXG_MAIN_SRC_FILES})
target_link_libraries(nexg_main nexg_utils nexg_graph_db)
install_nexg_target(nexg_main)




