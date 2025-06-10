add_library(nexg_compiler_main
        OBJECT
        attached_database.cpp
        client_context.cpp
        connection.cpp
        database.cpp
        database_manager.cpp
        plan_printer.cpp
        prepared_statement.cpp
        query_result.cpp
        query_summary.cpp
        version.cpp
        db_config.cpp
        settings.cpp)

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:nexg_compiler_main>
        PARENT_SCOPE)
