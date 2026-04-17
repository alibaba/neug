
file(GLOB GS_TEST_FILES RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)

set(NEUG_LIBRARIES neug)
find_package(OpenSSL REQUIRED)

set(BRPC_WITH_GLOG 1)

add_neug_test(test_extension test_extension.cc)

# Schema tests
add_neug_test(schema_test schema_test.cc)

# Logical delete tests
add_neug_test(logical_delete_test logical_delete_test.cc)

add_neug_test(test_indexer test_indexer.cc)

add_neug_test(test_connection test_connection.cc)

if (BUILD_HTTP_SERVER)
    add_neug_test(test_http_server test_http_server.cc)
    add_neug_test(test_db_svc test_db_svc.cc)
endif()