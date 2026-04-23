if (BUILD_HTTP_SERVER)
	add_neug_svc_test(
		transaction_test
		test_insert_transaction.cc
		test_update_transaction.cc
		test_acid.cc)
endif()
