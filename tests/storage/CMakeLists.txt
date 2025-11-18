set(storage_test_src test_mutable_csr.cc
                     test_ddl.cc 
                     test_export.cc 
                     test_open_graph.cc 
                     alter_property_test.cc 
                     test_set_property.cc 
                     test_checkpoint.cc 
                     test_memory_level.cc
                     test_property_graph.cc)
if (BUILD_HTTP_SERVER)
    list(APPEND storage_test_src test_recovery.cc)
endif()
add_neug_test(storage_test ${storage_test_src})