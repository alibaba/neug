add_library(nexg_function_date
        OBJECT
        date_functions.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:nexg_function_date>
        PARENT_SCOPE)
