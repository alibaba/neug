
file(GLOB_RECURSE METADATA_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(nexg_metadata_store SHARED ${METADATA_SRC_FILES})
target_link_libraries(nexg_metadata_store nexg_utils)
install_nexg_target(nexg_metadata_store)




