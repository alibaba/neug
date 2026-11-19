add_neug_test(test_c_api
    test_database.cc test_connection.cc test_result.cc test_graph.cc)

# This source MUST remain C, not a C++ translation unit with a .c suffix.
add_executable(c_api_smoke smoke.c)
set_target_properties(c_api_smoke PROPERTIES
    C_STANDARD 11 C_STANDARD_REQUIRED YES C_EXTENSIONS NO)
target_link_libraries(c_api_smoke PRIVATE neug)
add_test(NAME c_api_smoke COMMAND c_api_smoke
    "${CMAKE_CURRENT_BINARY_DIR}/smoke-db")
set_tests_properties(c_api_smoke PROPERTIES
    FIXTURES_REQUIRED c_api_smoke_db)
add_test(NAME c_api_smoke_setup COMMAND ${CMAKE_COMMAND} -E remove_directory
    "${CMAKE_CURRENT_BINARY_DIR}/smoke-db")
set_tests_properties(c_api_smoke_setup PROPERTIES
    FIXTURES_SETUP c_api_smoke_db)
add_test(NAME c_api_smoke_cleanup COMMAND ${CMAKE_COMMAND} -E remove_directory
    "${CMAKE_CURRENT_BINARY_DIR}/smoke-db")
set_tests_properties(c_api_smoke_cleanup PROPERTIES
    FIXTURES_CLEANUP c_api_smoke_db)
