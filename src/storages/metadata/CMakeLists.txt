
file(GLOB_RECURSE METADATA_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(neug_metadata_store SHARED ${METADATA_SRC_FILES})
target_link_libraries(neug_metadata_store neug_utils neug_rt_mutable_graph)
install_neug_target(neug_metadata_store)




