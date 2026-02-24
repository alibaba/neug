add_library(neug_function_arithmetic
        OBJECT
        multiply.cpp
        add.cpp
        subtract.cpp
        divide.cpp
        modulo.cpp
        negate.cpp
        abs.cpp)
add_dependencies(neug_function_arithmetic neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_function_arithmetic>
        PARENT_SCOPE)
