file(GLOB_RECURSE STORAGES_LOADER_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(neug_storages_loader OBJECT ${STORAGES_LOADER_SRC_FILES})
add_dependencies(neug_storages_loader neug_plan_proto)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_storages_loader>
        PARENT_SCOPE)