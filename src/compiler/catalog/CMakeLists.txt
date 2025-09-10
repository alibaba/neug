add_subdirectory(catalog_entry)

add_library(neug_catalog
        OBJECT
        catalog.cpp
        catalog_set.cpp
        function_signature_registry.cpp
        property_definition_collection.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_catalog>
        PARENT_SCOPE)
