
file(GLOB GS_TEST_FILES RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)


if (BUILD_ALL_IN_ONE)
        set(NEUG_LIBRARIES neug_libraries)
else()
        set(NEUG_LIBRARIES neug_compiler_shared neug_execution neug_main neug_brpc_proto)
endif()

add_executable(test_run_query test_run_query.cc)
target_link_libraries(test_run_query PRIVATE ${NEUG_LIBRARIES})

add_storage_test(storage_test test_ddl.cc test_export.cc test_open_graph.cc alter_property_test.cc test_set_property.cc)
target_link_libraries(storage_test
    PRIVATE
    neug_execution
    neug_main
    neug::GTest
    ${Protobuf_LIBRARIES}
)
