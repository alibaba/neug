add_library(neug_gopt
    OBJECT
    g_catalog.cpp
    g_ddl_converter.cpp
    g_expr_converter.cpp
    g_alias_manager.cpp
    g_type_converter.cpp
    g_query_converter.cpp
    g_precedence.cpp
    g_admin_convertor.cpp
    g_catalog_holder.cpp
    g_vfs_holder.cpp
)
add_dependencies(neug_gopt neug_plan_proto)

target_link_libraries(neug_gopt
    PUBLIC
    neug_plan_proto
    ${Protobuf_LIBRARIES})
target_link_libraries(neug_gopt PRIVATE  ${YAML_CPP_LIBRARIES})

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_gopt>
        PARENT_SCOPE)
