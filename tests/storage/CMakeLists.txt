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
