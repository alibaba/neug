set(TEST_OUT_OF_TREE_EXTENSION_OBJECT_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/test_out_of_tree_extension.cc")

build_extension_lib("test_out_of_tree")
target_link_libraries(neug_test_out_of_tree_extension PRIVATE neug)

# The duplicate-load test needs dlfcn/setenv (no MSVC equivalents) and passes
# the extension path via compile definitions: $<TARGET_FILE> expands with
# backslashes on Windows, and MSVC's preprocessor treats sequences like \t,
# \b, \U inside the macro value as escape sequences, corrupting the path.
# Windows does not build C++ test targets anyway (BUILD_TEST=OFF in CI); the
# extension itself builds and loads on Windows.
if(BUILD_TEST AND NOT WIN32)
    add_neug_test(test_duplicate_extension_load test_duplicate_load.cc)
    add_dependencies(test_duplicate_extension_load neug_test_out_of_tree_extension)
    target_compile_definitions(test_duplicate_extension_load PRIVATE
        TEST_OUT_OF_TREE_EXTENSION_HOME="$<TARGET_FILE_DIR:neug_test_out_of_tree_extension>/../.."
        TEST_OUT_OF_TREE_EXTENSION_PATH="$<TARGET_FILE:neug_test_out_of_tree_extension>")
endif()
