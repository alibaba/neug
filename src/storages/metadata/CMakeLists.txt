
file(GLOB_RECURSE METADATA_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(neug_metadata_store OBJECT ${METADATA_SRC_FILES})
add_dependencies(neug_metadata_store neug_utils)




