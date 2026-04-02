file(GLOB_RECURSE COLUMN_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(neug_storages_container OBJECT ${COLUMN_SOURCES})
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_storages_container>
        PARENT_SCOPE)