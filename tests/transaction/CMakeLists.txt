if (BUILD_HTTP_SERVER)
	add_neug_test(
		transaction_test
		test_insert_transaction.cc
		test_update_transaction.cc
		test_compact_transaction.cc
		test_transaction_release_order.cc
		test_wal_replay.cc
		test_acid.cc)
endif()
