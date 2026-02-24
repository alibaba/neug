add_library(neug_function_path
        OBJECT
        length_function.cpp
        nodes_function.cpp
        properties_function.cpp
        rels_function.cpp
        semantic_function.cpp)
add_dependencies(neug_function_path neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_function_path>
        PARENT_SCOPE)
