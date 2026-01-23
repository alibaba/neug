file(GLOB_RECURSE EXECUTION_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(neug_common OBJECT ${EXECUTION_SRC_FILES})
add_dependencies(neug_common neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_common>
        PARENT_SCOPE)