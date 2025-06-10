add_library(nexg_function_cast
        OBJECT
        cast_array.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:nexg_function_cast>
        PARENT_SCOPE)
