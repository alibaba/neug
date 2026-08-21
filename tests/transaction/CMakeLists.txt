add_neug_test(runtime_wait_test test_runtime_wait.cc)
add_neug_test(operation_gate_test test_operation_gate.cc)
add_neug_test(read_view_publication_test test_read_view_publication.cc)

if (BUILD_HTTP_SERVER)
	add_neug_test(
		transaction_test
		test_insert_transaction.cc
		test_snapshot_cow_write_transaction.cc
		test_compact_transaction.cc
		test_transaction_release_order.cc
		test_wal_replay.cc
		test_acid.cc)
endif()
