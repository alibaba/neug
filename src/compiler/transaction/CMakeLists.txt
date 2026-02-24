add_library(neug_compiler_transaction
        OBJECT
        transaction.cpp)
add_dependencies(neug_compiler_transaction neug_plan_proto)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_compiler_transaction>
        PARENT_SCOPE)
