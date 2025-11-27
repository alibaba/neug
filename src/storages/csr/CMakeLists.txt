file(GLOB_RECURSE CSR_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(neug_storages_csr OBJECT ${CSR_SOURCES})
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_storages_csr>
        PARENT_SCOPE)
add_dependencies(neug_storages_csr neug_plan_proto)