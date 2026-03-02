add_library(neug_gopt
    OBJECT
    g_catalog.cpp
    g_ddl_converter.cpp
    g_expr_converter.cpp
    g_alias_manager.cpp
    g_type_converter.cpp
    g_type_registration.cpp
    g_query_converter.cpp
    g_precedence.cpp
    g_catalog_holder.cpp
    g_vfs_holder.cpp
)
add_dependencies(neug_gopt neug_plan_proto)


set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_gopt>
        PARENT_SCOPE)
