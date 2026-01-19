
file(GLOB GS_TEST_FILES RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)

set(NEUG_LIBRARIES neug_libraries)
find_package(OpenSSL REQUIRED)

set(BRPC_WITH_GLOG 1)

add_executable(test_run_query test_run_query.cc)
target_link_libraries(test_run_query PRIVATE ${GLOG_LIBRARIES} ${NEUG_LIBRARIES} ${OPENSSL_LIBRARIES} ${CRYPTO_LIB} ${GFLAGS_LIBRARIES})

add_neug_test(version_manager_test test_version_manager.cc)

add_neug_test(test_extension test_extension.cc)

# Schema tests
add_neug_test(schema_test schema_test.cc)

# Logical delete tests
add_neug_test(logical_delete_test logical_delete_test.cc)

add_neug_test(test_connection test_connection.cc)

add_neug_test(test_http_server test_http_server.cc)