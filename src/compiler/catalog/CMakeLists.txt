add_subdirectory(catalog_entry)

add_library(neug_catalog
        OBJECT
        catalog.cpp
        catalog_set.cpp
        property_definition_collection.cpp)

add_dependencies(neug_catalog neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_catalog>
        PARENT_SCOPE)
