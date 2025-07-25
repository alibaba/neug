# There seems to be a circle dependency between graph_db, runtime_common and runtime_execute
# So currently we just add all the source files to the graph_db library
file(GLOB_RECURSE GRAPH_DB_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/app/*.cc"
  	                             "${CMAKE_CURRENT_SOURCE_DIR}/database/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/database/wal/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/runtime/common/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/runtime/execute/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/runtime/utils/*.cc")
if (BUILD_ALL_IN_ONE)
        add_library(neug_graph_db OBJECT ${GRAPH_DB_SRC_FILES})
        add_dependencies(neug_graph_db neug_plan_proto)
        set(ALL_OBJECT_FILES ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_graph_db> PARENT_SCOPE)
else()
        add_library(neug_graph_db SHARED ${GRAPH_DB_SRC_FILES})
        target_link_libraries(neug_graph_db neug_storages neug_utils neug_grape_lite ${Protobuf_LIBRARIES} ${GLOG_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
        if (APPLE)
                target_link_libraries(neug_graph_db absl::log_internal_message absl::log_internal_check_op)
        endif()
        install_neug_target(neug_graph_db)
endif()
target_include_directories(neug_graph_db PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}>)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}
        DESTINATION include/neug/engines
        FILES_MATCHING
        PATTERN "*.h"
)





