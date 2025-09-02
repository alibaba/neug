# There seems to be a circle dependency between graph_db, runtime_common and runtime_execute
# So currently we just add all the source files to the graph_db library
file(GLOB_RECURSE GRAPH_DB_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/app/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/runtime/common/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/runtime/execute/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/runtime/utils/*.cc")
if (BUILD_ALL_IN_ONE)
        add_library(neug_execution OBJECT ${GRAPH_DB_SRC_FILES})
        add_dependencies(neug_execution neug_plan_proto)
        set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_execution> PARENT_SCOPE)
else()
        add_library(neug_execution SHARED ${GRAPH_DB_SRC_FILES})
        target_link_libraries(neug_execution neug_storages neug_transaction neug_utils neug_grape_lite ${Protobuf_LIBRARIES} ${GLOG_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
        install_neug_target(neug_execution)
endif()
target_include_directories(neug_execution PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)




