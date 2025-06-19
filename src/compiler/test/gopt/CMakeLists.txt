add_compiler_test(
	gopt_test
	ddl_test.cpp
	dml_test.cpp
	pattern_test.cpp
	rbo_rule_test.cpp
	agg_test.cpp
	metadata_test.cpp
	path_test.cpp
	order_test.cpp
	expr_test.cpp)

target_link_libraries(gopt_test
    PRIVATE
    neug_gopt
    neug_plan_proto
    ${Protobuf_LIBRARIES}
)
