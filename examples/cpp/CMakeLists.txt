add_executable(simple_example simple_example.cc)
target_include_directories(simple_example PRIVATE ../../include)

find_package(OpenSSL REQUIRED)
target_link_libraries(simple_example PUBLIC neug_libraries)
target_link_libraries(simple_example PRIVATE ${GFLAGS_LIBRARIES} ${GLOG_LIBRARIES} ${YAML_CPP_LIBRARIES} ${OPENSSL_LIBRARIES} ${CRYPTO_LIB})  #${PROTOC_LIB}
