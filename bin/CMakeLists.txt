if (BUILD_HTTP_SERVER)
        # benchmark / rt_server intentionally do NOT link their own copy of
        # ${Protobuf_LIBRARIES}: protobuf is re-exported from libneug (PUBLIC).
        # Linking a second static libprotobuf.a here would create a second
        # protobuf runtime in the process and break descriptor pool sharing
        # ("Check failed: file != nullptr" / "File already exists in database").
        add_executable(rt_server rt_server.cc)
        message(STATUS "openssl library: ${OPENSSL_LIBRARIES}")
        target_link_libraries(rt_server PRIVATE neug)
        target_link_libraries(rt_server PRIVATE ${GLOG_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
        target_link_libraries(rt_server PRIVATE ${ARROW_LIB})
        install_without_export_neug_target(rt_server)

        add_executable(benchmark benchmark.cc)
        target_link_libraries(benchmark PUBLIC neug)
        target_link_libraries(benchmark PUBLIC ${GLOG_LIBRARIES})
        target_link_libraries(benchmark PRIVATE ${ARROW_LIB})
        install_without_export_neug_target(benchmark)
        if (APPLE)
                target_link_libraries(rt_server PRIVATE ${MACOS_DYLIBS})
                target_link_libraries(benchmark PRIVATE ${MACOS_DYLIBS})
        endif()
endif() 
