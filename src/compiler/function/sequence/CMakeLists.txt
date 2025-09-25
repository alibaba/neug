add_library(neug_function_sequence
        OBJECT
        sequence_functions.cpp)
add_dependencies(neug_function_sequence neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_function_sequence>
        PARENT_SCOPE)
