add_library(neug_function_date
        OBJECT
        date_functions.cpp)
add_dependencies(neug_function_date neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_function_date>
        PARENT_SCOPE)
