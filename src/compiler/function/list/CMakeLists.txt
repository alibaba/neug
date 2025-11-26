add_library(neug_list_function
        OBJECT
        list_concat_function.cpp
        list_contains_function.cpp
        list_creation.cpp
        list_extract_function.cpp
        list_unique_function.cpp)
add_dependencies(neug_list_function neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_list_function>
        PARENT_SCOPE)
