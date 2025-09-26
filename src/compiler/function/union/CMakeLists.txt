add_library(neug_function_union
        OBJECT
        union_extract_function.cpp
        union_tag_function.cpp
        union_value_function.cpp)
add_dependencies(neug_function_union neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_function_union>
        PARENT_SCOPE)
