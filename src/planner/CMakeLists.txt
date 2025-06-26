include_directories(${CMAKE_CURRENT_BINARY_DIR}/../utils)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../compiler/)
file(GLOB_RECURSE PLANNER_SOURCE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
if (NOT BUILD_JNI_PLANNER)
    # remove jni planner files
    list(FILTER PLANNER_SOURCE_FILES EXCLUDE REGEX "jni_graph_planner.cc")
endif()

if (NOT BUILD_COMPILER)
    # remove compiler files
    list(FILTER PLANNER_SOURCE_FILES EXCLUDE REGEX "gopt_planner.cc")
endif()

# if no source files are found, we still need to create the target
add_library(neug_planner SHARED ${PLANNER_SOURCE_FILES})

target_link_libraries(neug_planner ${GLOG_LIBRARIES} ${PROTOBUF_LIBRARIES} neug_utils neug_plan_proto)
if (BUILD_COMPILER)
    target_link_libraries(neug_planner neug_compiler_shared)
endif()
if (BUILD_JNI_PLANNER)
    target_link_libraries(neug_planner ${JAVA_JVM_LIBRARY} ${JNI_LIBRARIES})
    target_include_directories(neug_planner PUBLIC ${JAVA_INCLUDE_PATH} ${JAVA_INCLUDE_PATH2})
endif()
install_neug_target(neug_planner)