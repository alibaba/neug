file(GLOB_RECURSE COMMON_SRC_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/types/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/columns/*.cc")
add_library(neug_common OBJECT ${COMMON_SRC_FILES})
add_dependencies(neug_common neug_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_common>
        PARENT_SCOPE)