# End-to-end smoke test for the sampled_match extension.
# The test loads the built extension via LOAD (no INSTALL / no network),
# so it depends on the extension library being built first.

add_executable(sampled_match_test sampled_match_test.cpp)
target_include_directories(sampled_match_test PRIVATE
    ${CMAKE_SOURCE_DIR}/extension/sampled_match/include
    ${CMAKE_SOURCE_DIR}/include)
target_link_libraries(sampled_match_test PRIVATE neug)
add_dependencies(sampled_match_test neug_sampled_match_extension)
add_test(NAME sampled_match_test COMMAND sampled_match_test)
