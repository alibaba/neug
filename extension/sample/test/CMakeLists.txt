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
    gflags
)

# Test executable for LDBC SF01 dataset
add_executable(test_sample_match_ldbc test_sample_match_ldbc.cc)

target_include_directories(test_sample_match_ldbc PRIVATE
    ${CMAKE_SOURCE_DIR}/extension/sample/include
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include
)

target_link_libraries(test_sample_match_ldbc PRIVATE
    neug_libraries
    ${GLOG_LIBRARIES}
    gflags
)

# Test executable for OpenAIRE dataset
add_executable(test_sample_match_openaire test_sample_match_openaire.cc)

target_include_directories(test_sample_match_openaire PRIVATE
    ${CMAKE_SOURCE_DIR}/extension/sample/include
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include
)

target_link_libraries(test_sample_match_openaire PRIVATE
    neug_libraries
    ${GLOG_LIBRARIES}
    gflags
)

add_executable(test_install test_install.cc)

target_include_directories(test_install PRIVATE
    ${CMAKE_SOURCE_DIR}/extension/sample/include
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include
)

target_link_libraries(test_install PRIVATE
    neug_libraries
    ${GLOG_LIBRARIES}
    gflags
)

# Install the test executables
install(TARGETS test_sample_match test_sample_match_ldbc test_sample_match_openaire test_install
    RUNTIME DESTINATION bin
)
