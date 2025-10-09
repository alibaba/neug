
file(GLOB GS_TEST_FILES RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)

set(NEUG_LIBRARIES neug_libraries)
find_package(OpenSSL REQUIRED)

add_executable(test_run_query test_run_query.cc)
target_link_libraries(test_run_query PRIVATE ${GLOG_LIBRARIES} ${NEUG_LIBRARIES} ${OPENSSL_LIBRARIES} ${CRYPTO_LIB} ${GFLAGS_LIBRARIES})

add_storage_test(storage_test test_ddl.cc test_export.cc test_open_graph.cc alter_property_test.cc test_set_property.cc test_checkpoint.cc test_memory_level.cc)
target_link_libraries(storage_test
    PRIVATE
    ${GLOG_LIBRARIES}
    ${NEUG_LIBRARIES}
    neug::GTest
    ${Protobuf_LIBRARIES}
    ${ARROW_LIB}
    ${OPENSSL_LIBRARIES}
    ${CRYPTO_LIB}
    ${GFLAGS_LIBRARIES}
)

# Add version manager test
add_executable(version_manager_test test_version_manager.cc)
target_link_libraries(version_manager_test 
    PRIVATE
    ${GLOG_LIBRARIES}
    ${NEUG_LIBRARIES}
    neug::GTest
    ${Protobuf_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    ${CRYPTO_LIB}
    ${GFLAGS_LIBRARIES}
)

# Register the test with CTest
add_test(NAME version_manager_test COMMAND version_manager_test)
set_tests_properties(version_manager_test PROPERTIES TIMEOUT 60)

# Add vertex table benchmark
add_executable(vertex_table_benchmark vertex_table_benchmark.cc)
target_link_libraries(vertex_table_benchmark 
    PRIVATE
    ${GLOG_LIBRARIES}
    ${NEUG_LIBRARIES}
    neug::GTest
    ${Protobuf_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    ${CRYPTO_LIB}
    ${GFLAGS_LIBRARIES}
)

# Register the benchmark with CTest
add_test(NAME vertex_table_benchmark COMMAND vertex_table_benchmark)
set_tests_properties(vertex_table_benchmark PROPERTIES TIMEOUT 300)


add_executable(test_vertex_table test_vertex_table.cc)
target_link_libraries(test_vertex_table
    PRIVATE
    ${GLOG_LIBRARIES}
    ${NEUG_LIBRARIES}
    neug::GTest
    ${Protobuf_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    ${CRYPTO_LIB}
    ${GFLAGS_LIBRARIES}
)

# Register the test with CTest
add_test(NAME test_vertex_table COMMAND test_vertex_table)
set_tests_properties(test_vertex_table PROPERTIES TIMEOUT 60)


add_executable(test_extension test_extension.cc)
target_link_libraries(test_extension
    PRIVATE
    ${GLOG_LIBRARIES}
    ${NEUG_LIBRARIES}
    neug::GTest
    ${Protobuf_LIBRARIES}
    ${OPENSSL_LIBRARIES}
    ${CRYPTO_LIB}
    ${GFLAGS_LIBRARIES}
)

# Register the test with CTest
add_test(NAME test_extension COMMAND test_extension)
set_tests_properties(test_extension PROPERTIES TIMEOUT 60)
