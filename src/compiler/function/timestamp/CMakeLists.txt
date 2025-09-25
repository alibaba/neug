add_library(neug_timestamp_function
        OBJECT
        to_epoch_ms.cpp)
add_dependencies(neug_timestamp_function neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_timestamp_function>
        PARENT_SCOPE)
