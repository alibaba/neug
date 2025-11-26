add_library(neug_function_struct
        OBJECT
        struct_extract_function.cpp)
add_dependencies(neug_function_struct neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_function_struct>
        PARENT_SCOPE)
