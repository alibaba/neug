include_directories(${CMAKE_CURRENT_BINARY_DIR}/../utils)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../compiler/)
file(GLOB_RECURSE PLANNER_SOURCE_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

if (NOT BUILD_COMPILER)
    # remove compiler files
    list(FILTER PLANNER_SOURCE_FILES EXCLUDE REGEX "gopt_planner.cc")
endif()

# if no source files are found, we still need to create the target
if (BUILD_ALL_IN_ONE)
    add_library(neug_planner OBJECT ${PLANNER_SOURCE_FILES})
    add_dependencies(neug_planner neug_plan_proto)
    set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_planner> PARENT_SCOPE)
else()
    add_library(neug_planner SHARED ${PLANNER_SOURCE_FILES})
    target_link_libraries(neug_planner ${GLOG_LIBRARIES} ${PROTOBUF_LIBRARIES} neug_utils)
    if (BUILD_COMPILER)
        target_link_libraries(neug_planner neug_compiler_shared)
    endif()
    install_neug_target(neug_planner)
endif()

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/neug
        FILES_MATCHING
        PATTERN "*.h"
)



