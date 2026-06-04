# Memory-leak smoke tests.  These tests are GTEST_SKIP()-ed by default and
# only execute when the environment variable NEUG_RUN_MEMORY_LEAK_TESTS=1 is
# set.  See test_node_create.cc for details.
#
# `add_neug_test` registers the binary with ctest, but the test will report
# as PASS/SKIPPED when the env var is unset, so day-to-day `ctest` runs are
# unaffected.

add_neug_test(test_node_create test_node_create.cc)
add_neug_test(test_temporary_tables test_temporary_tables.cc)
