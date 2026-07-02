file(GLOB_RECURSE COLUMNAR_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(neug_columnar OBJECT ${COLUMNAR_SRC_FILES})
add_dependencies(neug_columnar neug_proto)
set(ALL_OBJECT_FILES
    ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_columnar>
    PARENT_SCOPE)
