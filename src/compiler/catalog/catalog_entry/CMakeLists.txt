add_library(neug_catalog_entry
        OBJECT
        catalog_entry.cpp
        catalog_entry_type.cpp
        function_catalog_entry.cpp
        table_catalog_entry.cpp
        node_table_catalog_entry.cpp
        rel_table_catalog_entry.cpp
        rel_group_catalog_entry.cpp
        scalar_macro_catalog_entry.cpp
        type_catalog_entry.cpp
        sequence_catalog_entry.cpp
        index_catalog_entry.cpp)
add_dependencies(neug_catalog_entry neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_catalog_entry>
        PARENT_SCOPE)
