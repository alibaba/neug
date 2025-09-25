add_subdirectory(utils)
add_subdirectory(storages)
add_subdirectory(transaction)

if (BUILD_COMPILER)
    add_subdirectory(compiler)
    add_subdirectory(main)
endif()
add_subdirectory(server)
add_subdirectory(execution)

if (BUILD_ALL_IN_ONE)
        message(STATUS "Compiler libraries: ${COMPILER_LIBRARIES}")
        add_library(neug_libraries SHARED ${ALL_OBJECT_FILES})
        target_link_libraries(neug_libraries PUBLIC ${Protobuf_LIBRARIES} ${COMPILER_LIBRARIES} ${GLOG_LIBRARIES} ${YAML_CPP_LIBRARIES} ${ARROW_LIB} neug_grape_lite)
        if (BUILD_HTTP_SERVER)
            target_link_libraries(neug_libraries PUBLIC neug_execution_service ${GFLAGS_LIBRARIES} ${OPENSSL_LIBRARIES} ${CRYPTO_LIB} ${BRPC_LIB} ${LEVELDB_LIB}) #${PROTOC_LIB}
        endif()
        if (APPLE)
            target_link_libraries(neug_libraries PUBLIC ${MACOS_DYLIBS})
        endif()
        if (WITH_MIMALLOC)
            target_link_libraries(neug_libraries PUBLIC mimalloc)
        endif()
        target_include_directories(neug_libraries PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/compiler>
            $<INSTALL_INTERFACE:include/neug/compiler>)
        install_neug_target(neug_libraries)
        if (ENABLE_BACKTRACES)
            target_link_libraries(neug_libraries PUBLIC cpptrace::cpptrace)
        endif()
        if (BUILD_HTTP_SERVER)
            find_package(absl)
            if (absl_FOUND)
                # When protobuf is installed on macos with `brew install protobuf`, some absl symbols are not exposed correctly.
                # https://github.com/protocolbuffers/protobuf/issues/16314
                target_link_libraries(
                    neug_libraries PRIVATE absl::log_internal_message absl::log_internal_check_op absl::status
                )
            endif()
        endif()
endif()
