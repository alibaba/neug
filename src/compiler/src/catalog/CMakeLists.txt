add_subdirectory(catalog_entry)

add_library(nexg_catalog
        OBJECT
        catalog.cpp
        catalog_set.cpp
        property_definition_collection.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:nexg_catalog>
        PARENT_SCOPE)
