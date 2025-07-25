
add_subdirectory(graph_db)
set(ENGINES_ALL_OBJECT_FILES "")
if (BUILD_HTTP_SERVER)
    if (NOT HTTP_SERVER_TYPE STREQUAL "hiactor" AND NOT HTTP_SERVER_TYPE STREQUAL "brpc")
        message(FATAL_ERROR "Invalid HTTP_SERVER_TYPE: ${HTTP_SERVER_TYPE}. It should be either 'hiactor' or 'brpc'.")
    endif()
    if (HTTP_SERVER_TYPE STREQUAL "hiactor")
        find_package (Hiactor)
        include (${Hiactor_CODEGEN_CMAKE_FILE})

        hiactor_codegen (server_actor_autogen server_actor_autogen_files
            SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/http_server/
            INCLUDE_PATHS ${Hiactor_INCLUDE_DIR},${CMAKE_CURRENT_SOURCE_DIR}/../../,${CMAKE_CURRENT_BINARY_DIR}/../utils/)

        file(GLOB_RECURSE SERVER_FILES "${CMAKE_CURRENT_SOURCE_DIR}/http_server/*.cc")
        list(FILTER SERVER_FILES EXCLUDE REGEX ".*generated.*")

        add_library(neug_graph_db_service STATIC
            ${CMAKE_CURRENT_SOURCE_DIR}/graph_db_service.cc
            ${SERVER_FILES} ${server_actor_autogen_files})
        target_compile_definitions(neug_graph_db_service PRIVATE HTTP_SERVER_TYPE="${HTTP_SERVER_TYPE}")
        target_compile_definitions(neug_graph_db_service PUBLIC SEASTAR_SCHEDULING_GROUPS_COUNT=${Seastar_SCHEDULING_GROUPS_COUNT})
        target_compile_options (neug_graph_db_service PUBLIC -Wno-attributes)

        target_include_directories(neug_graph_db_service PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/http_server)

        add_dependencies(neug_graph_db_service neug_utils) # Make sure neug_utils is built before neug_server
        add_dependencies(neug_graph_db_service server_actor_autogen)

        get_target_property(seastar_options Hiactor::seastar INTERFACE_COMPILE_OPTIONS)
        include(CheckCXXCompilerFlag)
        check_cxx_compiler_flag("-Wno-maybe-uninitialized" COMPILER_SUPPORTS_WNO_MAYBE_UNINITIALIZED)
        if (NOT COMPILER_SUPPORTS_WNO_MAYBE_UNINITIALIZED)
            list(FILTER seastar_options EXCLUDE REGEX "-Wno-maybe-uninitialized")
            message(STATUS "remove -Wno-maybe-uninitialized from seastar_options, since not supported by ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")
        endif ()
        set_target_properties(Hiactor::seastar PROPERTIES INTERFACE_COMPILE_OPTIONS "${seastar_options}")
        target_link_libraries(neug_graph_db_service PUBLIC Hiactor::hiactor)
        install_neug_target(neug_graph_db_service)

    elseif (HTTP_SERVER_TYPE STREQUAL "brpc")
#        if (BUILD_ALL_IN_ONE)
#            add_library(neug_graph_db_service OBJECT
#                ${CMAKE_CURRENT_SOURCE_DIR}/graph_db_service.cc
#                ${CMAKE_CURRENT_SOURCE_DIR}/brpc_server/brpc_http_hdl_mgr.cc)
#            set(ENGINES_ALL_OBJECT_FILES ${ENGINES_ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_graph_db_service>) 
#        else()
            add_library(neug_graph_db_service STATIC
                ${CMAKE_CURRENT_SOURCE_DIR}/graph_db_service.cc
                ${CMAKE_CURRENT_SOURCE_DIR}/brpc_server/brpc_http_hdl_mgr.cc)
            install_neug_target(neug_graph_db_service)
#        endif()
        target_compile_definitions(neug_graph_db_service PUBLIC HTTP_SERVER_TYPE="${HTTP_SERVER_TYPE}")
    endif()
    
 #   if (NOT BUILD_ALL_IN_ONE)
#        target_link_libraries(neug_graph_db_service PUBLIC neug_graph_db neug_brpc_proto)
#    endif()
    target_include_directories(neug_graph_db_service PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/../utils/)

endif()
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} ${ENGINES_ALL_OBJECT_FILES} PARENT_SCOPE)
