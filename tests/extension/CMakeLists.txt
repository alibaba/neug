function(add_extension_test EXTENSION_NAME)
    find_package(OpenSSL REQUIRED)
    set(TEST_NAME ${EXTENSION_NAME}_extension_test)
    set(SRCS ${ARGN})
    add_executable(${TEST_NAME} ${SRCS})
    target_link_libraries(${TEST_NAME} PUBLIC neug::GTest neug_${EXTENSION_NAME}_extension neug_libraries
        ${GLOG_LIBRARIES} ${ARROW_LIB} ${OPENSSL_LIBRARIES} 
        ${CRYPTO_LIB} ${GFLAGS_LIBRARIES})
    if (ENABLE_GCOV)
        target_compile_options(${TEST_NAME} PRIVATE --coverage)
        target_link_options(${TEST_NAME} PRIVATE --coverage)
    endif()
    include(GoogleTest)
	add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
	gtest_add_tests(TARGET ${TEST_NAME} TEST_LIST ${TEST_NAME}_tests)
endfunction()

if ("json" IN_LIST BUILD_EXTENSIONS)
    add_extension_test(json json_test.cpp)

    find_package(OpenSSL REQUIRED)
    add_executable(test_json_extension
        ${CMAKE_SOURCE_DIR}/extension/json/test/test_json_extension.cc)
    target_link_libraries(test_json_extension PRIVATE
        neug_libraries
        ${GLOG_LIBRARIES}
        ${ARROW_LIB}
        ${OPENSSL_LIBRARIES}
        ${CRYPTO_LIB}
        ${GFLAGS_LIBRARIES}
        gflags)
    set_target_properties(test_json_extension PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/extension/json/test")
endif()