set(TEST_OUT_OF_TREE_EXTENSION_OBJECT_FILES
    "${CMAKE_CURRENT_SOURCE_DIR}/test_out_of_tree_extension.cc")

build_extension_lib("test_out_of_tree")
target_link_libraries(neug_test_out_of_tree_extension PRIVATE neug)
