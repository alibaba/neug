add_executable(simple_example simple_example.cc)
target_include_directories(simple_example PRIVATE ../../include)

target_link_libraries(simple_example PUBLIC neug)
target_link_libraries(simple_example PRIVATE ${GLOG_LIBRARIES} ${YAML_CPP_LIBRARIES} ${Protobuf_LIBRARIES})
