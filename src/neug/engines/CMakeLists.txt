
add_subdirectory(graph_db)
set(ENGINES_ALL_OBJECT_FILES "")
if (BUILD_HTTP_SERVER)
    add_library(neug_graph_db_service STATIC
            ${CMAKE_CURRENT_SOURCE_DIR}/graph_db_service.cc
            ${CMAKE_CURRENT_SOURCE_DIR}/brpc_server/brpc_http_hdl_mgr.cc)
    install_neug_target(neug_graph_db_service)
    target_include_directories(neug_graph_db_service PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/../utils/)
endif()
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} ${ENGINES_ALL_OBJECT_FILES} PARENT_SCOPE)
