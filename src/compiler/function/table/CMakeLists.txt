add_library(neug_table_function
        OBJECT
        bind_data.cpp
        bind_input.cpp
        table_function.cpp)
add_dependencies(neug_table_function neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_table_function>
        PARENT_SCOPE)
