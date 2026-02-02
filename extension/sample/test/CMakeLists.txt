# Test for sample extension

# Test executable for subgraph matching
add_executable(test_sample_match test_sample_match.cc)

target_include_directories(test_sample_match PRIVATE
    ${CMAKE_SOURCE_DIR}/extension/sample/include
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include
)

target_link_libraries(test_sample_match PRIVATE
    neug_libraries
    ${GLOG_LIBRARIES}
)

# Install the test executable
install(TARGETS test_sample_match
    RUNTIME DESTINATION bin
)
