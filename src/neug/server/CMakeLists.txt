if (BUILD_HTTP_SERVER)
    add_library(neug_graph_db_service STATIC
            ${CMAKE_CURRENT_SOURCE_DIR}/graph_db_service.cc
            ${CMAKE_CURRENT_SOURCE_DIR}/brpc_http_hdl_mgr.cc)
    install_neug_target(neug_graph_db_service)
    add_dependencies(neug_graph_db_service neug_plan_proto)
endif()