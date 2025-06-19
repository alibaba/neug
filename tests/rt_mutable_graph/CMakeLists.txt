
file(GLOB GS_TEST_FILES RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)

set(STRING_LIST
    "test_open_graph"
    "test_run_query"
    "test_delete_columns"
    "test_ddl"
    "test_gopt_planner"
)

foreach(f ${GS_TEST_FILES})
        string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
        set(T_NAME ${CMAKE_MATCH_1})
        message(STATUS "Found rt_mutable_graph test - " ${T_NAME})
        # if matches any string in STRING_LIST, link with neug_planner and neug_main
        if (T_NAME IN_LIST STRING_LIST)
                if (TARGET neug_main AND TARGET neug_planner)       
                        add_executable(${T_NAME} ${CMAKE_CURRENT_SOURCE_DIR}/${T_NAME}.cc)
                        target_link_libraries(${T_NAME} neug_graph_db neug_planner neug_main)
                endif()
        else()
                add_executable(${T_NAME} ${CMAKE_CURRENT_SOURCE_DIR}/${T_NAME}.cc)
                target_link_libraries(${T_NAME} neug_graph_db)
        endif()
endforeach()
