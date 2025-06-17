include_directories(${CMAKE_CURRENT_BINARY_DIR}/../utils)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../compiler/)
file(GLOB_RECURSE PLANNER_SOURCE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
if (NOT BUILD_JNI_PLANNER)
    # remove jni planner files
    list(FILTER PLANNER_SOURCE_FILES EXCLUDE REGEX "jni_graph_planner.cc")
endif()

add_library(nexg_planner SHARED ${PLANNER_SOURCE_FILES})

target_link_libraries(nexg_planner ${GLOG_LIBRARIES} ${PROTOBUF_LIBRARIES} nexg_utils nexg_plan_proto nexg_compiler_shared)
if (BUILD_JNI_PLANNER)
    target_link_libraries(nexg_planner ${JAVA_JVM_LIBRARY} ${JNI_LIBRARIES})
    target_include_directories(nexg_planner PUBLIC ${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2})
endif()
install_nexg_target(nexg_planner)