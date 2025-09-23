add_library(neug_common_arrow
        OBJECT
        arrow_array_scan.cpp
        arrow_converter.cpp
        arrow_null_mask_tree.cpp
        arrow_row_batch.cpp
        arrow_type.cpp)
add_dependencies(neug_common_arrow neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_common_arrow>
        PARENT_SCOPE)
