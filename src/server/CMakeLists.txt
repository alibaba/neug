if (BUILD_HTTP_SERVER)
    set(NEUG_SERVER_FILES
        brpc_http_handler.cc
        brpc_transport.cc
        bthread_runtime_wait.cc
        neug_db_service.cc
        service_transaction_manager.cc
        tp_execution_slot_pool.cc
        tp_service_runtime.cc
    )
    add_library(neug_execution_service OBJECT ${NEUG_SERVER_FILES})
    add_dependencies(neug_execution_service neug_proto neug_utils)
    target_include_directories(neug_execution_service PRIVATE "${CMAKE_CURRENT_BINARY_DIR}/../utils/neug/generated/proto/plan")
    set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_execution_service> PARENT_SCOPE)
endif()
