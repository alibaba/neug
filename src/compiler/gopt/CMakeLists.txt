add_library(neug_gopt
    OBJECT
    g_catalog.cpp
    g_ddl_converter.cpp
    g_expr_converter.cpp
    g_alias_manager.cpp
    g_type_converter.cpp
    g_query_converter.cpp
    g_precedence.cpp
)

target_link_libraries(neug_gopt
    PUBLIC
    neug_plan_proto
    ${Protobuf_LIBRARIES}
    ${YAML_CPP_LIBRARIES})

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_gopt>
        PARENT_SCOPE)
