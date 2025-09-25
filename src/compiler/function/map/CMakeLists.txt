add_library(neug_function_map
        OBJECT
        map_creation_function.cpp
        map_extract_function.cpp
        map_keys_function.cpp
        map_values_function.cpp)
add_dependencies(neug_function_map neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_function_map>
        PARENT_SCOPE)
