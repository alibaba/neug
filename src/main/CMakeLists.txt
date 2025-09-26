file(GLOB_RECURSE NEUG_MAIN_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc"
                                        "${CMAKE_CURRENT_SOURCE_DIR}/app/*.cc")
add_library(neug_main OBJECT ${NEUG_MAIN_SRC_FILES})
add_dependencies(neug_main neug_plan_proto)
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_main> PARENT_SCOPE)