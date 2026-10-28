
file(GLOB GS_TEST_FILES RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)

set(NEUG_LIBRARIES neug)

set(BRPC_WITH_GLOG 1)

add_neug_test(test_extension test_extension.cc)
target_compile_definitions(test_extension PRIVATE
    NEUG_LITE=$<BOOL:${NEUG_LITE}>)

# Schema tests
add_neug_test(schema_test schema_test.cc)

# Logical delete tests
add_neug_test(logical_delete_test logical_delete_test.cc)

add_neug_test(test_indexer test_indexer.cc)

add_neug_test(test_mmap_container test_mmap_container.cc)

add_neug_test(test_connection test_connection.cc)

add_neug_test(test_query_result test_query_result.cc)

add_neug_test(test_query_result_list test_query_result_list.cc)

add_neug_test(test_column test_column.cc)

add_neug_test(test_explain_profile test_explain_profile.cpp)

if (BUILD_HTTP_SERVER)
    if (WIN32)
        # test_http_server drives the BRPC channel and test_db_svc links
        # brpc/bthread; on Windows the httplib transport is exercised by
        # httplib_load_client instead. That tool is a manual load client
        # requiring <uri> <thread_num> <request_num> arguments and a running
        # server, so it is built for manual runs but not registered with
        # CTest (an argumentless ctest invocation would just exit -1).
        add_executable(httplib_load_client httplib_load_client.cc)
        # neug_static contains the same object files as neug.dll but exposes
        # all symbols, which the client's internal-API usage needs. No gtest:
        # the client has no test cases of its own.
        target_link_libraries(httplib_load_client PUBLIC
            neug_static ${ARROW_LIB} ${OPENSSL_LIBRARIES} ${CRYPTO_LIB})
    else()
        add_neug_test(test_http_server test_http_server.cc)
        add_neug_test(test_db_svc test_db_svc.cc)
    endif()
    add_neug_test(test_array_service test_array_service.cc)
endif()
