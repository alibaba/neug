add_library(neug_function_array
        OBJECT
        array_functions.cpp
        array_value.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_function_array>
        PARENT_SCOPE)
