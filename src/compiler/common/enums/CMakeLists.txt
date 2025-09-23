add_library(neug_common_enums
        OBJECT
        accumulate_type.cpp
        path_semantic.cpp
        query_rel_type.cpp
        rel_direction.cpp
        rel_multiplicity.cpp
        scan_source_type.cpp
        table_type.cpp
        transaction_action.cpp
        drop_type.cpp
        extend_direction_util.cpp
        conflict_action.cpp)
add_dependencies(neug_common_enums neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_common_enums>
        PARENT_SCOPE)
