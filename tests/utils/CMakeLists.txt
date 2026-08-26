add_neug_test(
	utils_test
	test_table.cc
	test_types.cc
	test_reader.cc
	test_utils.cc
	test_exception.cc
	test_file_system.cc
	test_sniffer.cc
	test_input_stream.cc
	test_output_stream.cc
	json_test.cc)
if(NOT WIN32)
    target_sources(utils_test PRIVATE test_file_utils.cc)
endif()
