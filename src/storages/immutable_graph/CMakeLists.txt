file(GLOB_RECURSE IMMUTABLE_GRAPH_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(nexg_immutable_graph SHARED ${IMMUTABLE_GRAPH_SRC_FILES})
target_link_libraries(nexg_immutable_graph ${LIBGRAPELITE_LIBRARIES})
install_nexg_target(nexg_immutable_graph)