
add_library(kuzu_gopt
    OBJECT
    g_catalog.cpp
    g_storage_manager.cpp
    g_transaction_manager.cpp
)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:kuzu_gopt>
        PARENT_SCOPE)
