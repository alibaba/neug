add_library(nexg_common_vector
        OBJECT
        auxiliary_buffer.cpp
        value_vector.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:nexg_common_vector>
        PARENT_SCOPE)
