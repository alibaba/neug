add_kuzu_test(gopt_test gopt_test.cpp)
target_link_libraries(gopt_test
    PRIVATE
    kuzu_gopt
    nexg_plan_proto
    ${Protobuf_LIBRARIES}
)