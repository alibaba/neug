add_library(neug_function_cast
        OBJECT
        cast_array.cpp)
add_dependencies(neug_function_cast neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_function_cast>
        PARENT_SCOPE)
