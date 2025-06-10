add_compiler_test(gopt_test gopt_test.cpp)
target_link_libraries(gopt_test
    PRIVATE
    nexg_gopt
    nexg_plan_proto
    ${Protobuf_LIBRARIES}
)