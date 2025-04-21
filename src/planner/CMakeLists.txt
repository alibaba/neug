include_directories(${CMAKE_CURRENT_BINARY_DIR}/../utils)
file(GLOB_RECURSE PLANNER_SOURCE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")
if (NOT BUILD_JNI_PLANNER)
    # remove jni planner files
    list(FILTER PLANNER_SOURCE_FILES EXCLUDE REGEX "jni_graph_planner.cc")
endif()

add_library(nexg_planner SHARED ${PLANNER_SOURCE_FILES})
target_link_libraries(nexg_planner ${CMAKE_JNI_LINKER_FLAGS} ${JAVA_JVM_LIBRARY} ${JNI_LIBRARIES})
install_nexg_target(nexg_planner)