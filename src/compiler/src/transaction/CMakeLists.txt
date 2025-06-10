add_library(nexg_transaction
        OBJECT
        transaction.cpp
        transaction_context.cpp
        transaction_manager.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:nexg_transaction>
        PARENT_SCOPE)
