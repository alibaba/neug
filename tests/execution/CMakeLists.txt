add_neug_test(
	execution_test
	test_logical_expr.cc
	test_scalar_function_expr.cc
	test_plan_parser.cc
	test_query_cache.cc
	test_value.cc
	test_value_column.cc
	test_runtime_column.cc)

# Exercise first initialization before any database fixture initializes it.
add_neug_test(test_plan_parser_init test_plan_parser_init.cc)
