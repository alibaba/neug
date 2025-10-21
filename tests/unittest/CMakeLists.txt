
file(GLOB GS_TEST_FILES RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)

set(NEUG_LIBRARIES neug_libraries)
find_package(OpenSSL REQUIRED)

add_executable(test_run_query test_run_query.cc)
target_link_libraries(test_run_query PRIVATE ${GLOG_LIBRARIES} ${NEUG_LIBRARIES} ${OPENSSL_LIBRARIES} ${CRYPTO_LIB} ${GFLAGS_LIBRARIES})

set(storage_test_src test_ddl.cc 
                     test_export.cc 
                     test_open_graph.cc 
                     alter_property_test.cc 
                     test_set_property.cc 
                     test_checkpoint.cc 
                     test_memory_level.cc 
                     test_acid.cc)
if (BUILD_HTTP_SERVER)
    list(APPEND storage_test_src test_recovery.cc)
endif()
add_neug_test(storage_test ${storage_test_src})

add_neug_test(version_manager_test test_version_manager.cc)
add_neug_test(csr_test test_csr_stream_ops.cc test_csr_batch_ops.cc)
add_neug_test(edge_table_test test_edge_table.cc)

# Add vertex table benchmark
add_neug_test(vertex_table_benchmark vertex_table_benchmark.cc)

add_neug_test(test_vertex_table test_vertex_table.cc)

add_neug_test(test_extension test_extension.cc)