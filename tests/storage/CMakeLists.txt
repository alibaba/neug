set(storage_test_src test_ddl.cc
                     test_export.cc
                     test_open_graph.cc
                     alter_property_test.cc
                     test_set_property.cc
                     test_memory_level.cc
                     test_property_graph.cc)
if (BUILD_HTTP_SERVER)
    list(APPEND storage_test_src test_recovery.cc test_checkpoint.cc)
endif()
add_neug_test(storage_test ${storage_test_src})

add_neug_test(ap_index_test test_ap_index.cc)

add_neug_test(e2e_index_test test_e2e_index.cc)
add_dependencies(e2e_index_test neug_test_vec_index_extension)
target_compile_definitions(e2e_index_test PRIVATE
    NEUG_TEST_VEC_INDEX_EXTENSION_HOME="${NEUG_TEST_VEC_INDEX_EXTENSION_HOME}")

add_neug_test(tp_index_test test_tp_index.cc)

add_neug_test(csr_test test_csr_stream_ops.cc
                       test_csr_batch_ops.cc
                       test_mutable_csr.cc
                       test_immutable_csr.cc)

add_neug_test(test_vertex_table test_vertex_table.cc)

add_neug_test(edge_table_test test_edge_table.cc)

add_neug_test(graph_view_test test_graph_view.cc)

add_neug_test(graph_snapshot_store_test test_graph_snapshot_store_concurrency.cc)

add_neug_test(temporary_graph_test test_temporary_graph.cc)

add_neug_test(copy_temp_test test_copy_temp.cc)
