add_library(neug_function_internal_id
        OBJECT
        internal_id_creation_function.cpp)
add_dependencies(neug_function_internal_id neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_function_internal_id>
        PARENT_SCOPE)
