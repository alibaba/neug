
add_subdirectory(graph_db)
if (BUILD_HTTP_SERVER)
    add_subdirectory(http_server)
endif()
set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} PARENT_SCOPE)
