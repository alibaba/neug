
file(GLOB GS_TEST_FILES RELATIVE "${CMAKE_CURRENT_SOURCE_DIR}" "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")


foreach(f ${GS_TEST_FILES})
        string(REGEX MATCH "^(.*)\\.[^.]*$" dummy ${f})
        set(T_NAME ${CMAKE_MATCH_1})
        message(STATUS "Found rt_mutable_graph test - " ${T_NAME})
        add_executable(${T_NAME} ${CMAKE_CURRENT_SOURCE_DIR}/${T_NAME}.cc)
        target_link_libraries(${T_NAME}  nexg_graph_db)
        #if name is test_open_graph
        if (${T_NAME} MATCHES "test_open_graph")
                message(STATUS "Found open graph test - " ${T_NAME})
                target_link_libraries(${T_NAME} nexg_planner nexg_main)
        endif()
        if (${T_NAME} MATCHES "test_run_query")
                message(STATUS "Found run query test - " ${T_NAME})
                target_link_libraries(${T_NAME} nexg_planner nexg_main)
        endif()
        if (${T_NAME} MATCHES "test_delete_columns")
                message(STATUS "Found delete columns test - " ${T_NAME})
                target_link_libraries(${T_NAME} nexg_planner nexg_main)
        endif()

        if (${T_NAME} MATCHES "test_ddl")
                message(STATUS "Found ddl test - " ${T_NAME})
                target_link_libraries(${T_NAME} nexg_planner nexg_main)
        endif()
endforeach()
