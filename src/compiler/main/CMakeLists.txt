add_library(neug_compiler_main
        OBJECT
        client_context.cpp
        database.cpp
        plan_printer.cpp
        prepared_statement.cpp
        query_summary.cpp
        storage_manager.cpp
        settings.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_compiler_main>
        PARENT_SCOPE)
