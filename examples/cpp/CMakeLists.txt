add_executable(simple_example simple_example.cc)
target_include_directories(simple_example PRIVATE ../../include)

target_link_libraries(simple_example PUBLIC neug_libraries)
