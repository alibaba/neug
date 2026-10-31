add_executable(simple_example simple_example.cc)
target_include_directories(simple_example PRIVATE ../../include)

target_link_libraries(simple_example PUBLIC neug)
target_link_libraries(simple_example PRIVATE ${GLOG_LIBRARIES} ${YAML_CPP_LIBRARIES} ${Protobuf_LIBRARIES})

# End-to-end write benchmark for the chunked-COW storage optimization. Depends
# only on the public NeugDB API so the same source builds on both the legacy
# baseline and the optimized tree (see chunk_e2e_benchmark.cc header).
add_executable(chunk_e2e_benchmark chunk_e2e_benchmark.cc)
target_include_directories(chunk_e2e_benchmark PRIVATE ../../include)
target_link_libraries(chunk_e2e_benchmark PUBLIC neug)
target_link_libraries(chunk_e2e_benchmark PRIVATE ${GLOG_LIBRARIES} ${YAML_CPP_LIBRARIES} ${Protobuf_LIBRARIES})
