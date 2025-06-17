# There seems to be a circle dependency between graph_db, runtime_common and runtime_execute
# So currently we just add all the source files to the graph_db library
file(GLOB_RECURSE GRAPH_DB_SRC_FILES "${CMAKE_CURRENT_SOURCE_DIR}/app/*.cc"
  	                             "${CMAKE_CURRENT_SOURCE_DIR}/database/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/database/wal/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/runtime/common/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/runtime/execute/*.cc"
                                     "${CMAKE_CURRENT_SOURCE_DIR}/runtime/utils/*.cc")

add_library(neug_graph_db SHARED ${GRAPH_DB_SRC_FILES})

target_include_directories(neug_graph_db PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}> $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/../../utils>)
target_link_libraries(neug_graph_db neug_rt_mutable_graph ${LIBGRAPELITE_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
target_link_libraries(neug_graph_db neug_plan_proto ${Protobuf_LIBRARIES} ${GLOG_LIBRARIES})
if (APPLE)
        target_link_libraries(neug_graph_db absl::log_internal_message absl::log_internal_check_op)
endif()
install_neug_target(neug_graph_db)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/database/graph_db.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/graph_db_session.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/insert_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/read_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/single_edge_insert_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/single_vertex_insert_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/update_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/compact_transaction.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/version_manager.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/transaction_utils.h
              ${CMAKE_CURRENT_SOURCE_DIR}/database/graph_db_operations.h
        DESTINATION include/src/engines/graph_db/database)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/app/app_base.h
        DESTINATION include/src/engines/graph_db/app)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/app/hqps_app.h
        DESTINATION include/src/engines/graph_db/app)

install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/database/wal/wal.h
        ${CMAKE_CURRENT_SOURCE_DIR}/database/wal/local_wal_parser.h
        ${CMAKE_CURRENT_SOURCE_DIR}/database/wal/local_wal_writer.h
        DESTINATION include/src/engines/graph_db/database/wal)



