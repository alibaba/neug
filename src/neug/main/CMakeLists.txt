file(GLOB_RECURSE neug_MAIN_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../utils)
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../compiler)
if (BUILD_ALL_IN_ONE)
    add_library(neug_main OBJECT ${neug_MAIN_SRC_FILES})
    add_dependencies(neug_main neug_plan_proto)
    set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_main> PARENT_SCOPE)
else()
    add_library(neug_main SHARED ${neug_MAIN_SRC_FILES})
    target_link_libraries(neug_main neug_utils neug_graph_db neug_planner)
    if (BUILD_COMPILER)
        target_link_libraries(neug_main neug_compiler_shared)
    endif()
    install_without_export_neug_target(neug_main)
endif()

if (BUILD_HTTP_SERVER AND HTTP_SERVER_TYPE STREQUAL "brpc")
    target_compile_definitions(neug_main PUBLIC HTTP_SERVER_TYPE="${HTTP_SERVER_TYPE}")
endif()

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/neug_db.h
        ${CMAKE_CURRENT_SOURCE_DIR}/connection.h
        ${CMAKE_CURRENT_SOURCE_DIR}/file_lock.h
        ${CMAKE_CURRENT_SOURCE_DIR}/query_processor.h
        ${CMAKE_CURRENT_SOURCE_DIR}/query_result.h
        DESTINATION include/neug/main)
