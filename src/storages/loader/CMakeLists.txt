file(GLOB_RECURSE STORAGES_LOADER_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
if (NOT BUILD_ODPS_FRAGMENT_LOADER)
        # exclude odps_fragment_loader.cc
        message(STATUS "exclude odps_fragment_loader.cc")
        list(REMOVE_ITEM STORAGES_LOADER_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/odps_fragment_loader.cc")
endif()
add_library(neug_storages_loader OBJECT ${STORAGES_LOADER_SRC_FILES})
add_dependencies(neug_storages_loader neug_plan_proto)
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_storages_loader>
        PARENT_SCOPE)