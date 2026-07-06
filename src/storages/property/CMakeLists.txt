file(GLOB_RECURSE PROPERTY_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
add_library(neug_storages_property OBJECT ${PROPERTY_SOURCES})
set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_storages_property>
        PARENT_SCOPE)
add_dependencies(neug_storages_property neug_proto)
