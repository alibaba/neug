add_library(neug_timestamp_function
        OBJECT
        to_epoch_ms.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_timestamp_function>
        PARENT_SCOPE)
