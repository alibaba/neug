add_library(neug_function_pattern
        OBJECT
        cost_function.cpp
        id_function.cpp
        label_function.cpp
        start_end_node_function.cpp)
add_dependencies(neug_function_pattern neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_function_pattern>
        PARENT_SCOPE)
