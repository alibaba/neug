add_library(nexg_gopt
    OBJECT
    g_catalog.cpp
    g_storage_manager.cpp
    g_transaction_manager.cpp
    g_ddl_converter.cpp
    g_expr_converter.cpp
    g_alias_manager.cpp
    g_type_converter.cpp
    g_query_converter.cpp
)

target_link_libraries(nexg_gopt
    PUBLIC
    ${YAML_CPP_LIBRARIES}
    nexg_plan_proto
    ${Protobuf_LIBRARIES})

set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:nexg_gopt>
        PARENT_SCOPE)
