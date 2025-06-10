add_library(nexg_function_arithmetic
        OBJECT
        multiply.cpp
        add.cpp
        subtract.cpp
        divide.cpp
        modulo.cpp
        negate.cpp
        abs.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:nexg_function_arithmetic>
        PARENT_SCOPE)
