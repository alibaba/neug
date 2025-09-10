add_library(neug_compiler_transaction
        OBJECT
        transaction.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_compiler_transaction>
        PARENT_SCOPE)
