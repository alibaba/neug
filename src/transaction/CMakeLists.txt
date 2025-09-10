file(GLOB_RECURSE TRANSACTION_SOURCES "${CMAKE_CURRENT_SOURCE_DIR}/*.cc" "${CMAKE_CURRENT_SOURCE_DIR}/wal/*.cc")
if (BUILD_ALL_IN_ONE)
    add_library(neug_transaction OBJECT ${TRANSACTION_SOURCES})
    set(ALL_OBJECT_FILES
        ${ALL_OBJECT_FILES} $<TARGET_OBJECTS:neug_transaction>
        PARENT_SCOPE)
else()
    add_library(neug_transaction SHARED ${TRANSACTION_SOURCES})
    target_link_libraries(neug_transaction neug_storages neug_utils ${CMAKE_THREAD_LIBS_INIT} ${GLOG_LIBRARIES})
    install_neug_target(neug_transaction)
endif()

add_dependencies(neug_transaction neug_plan_proto)