if (BUILD_HTTP_SERVER)
	add_neug_test(
		transaction_test
		test_insert_transaction.cc
		test_update_transaction.cc
		test_acid.cc)
endif()
