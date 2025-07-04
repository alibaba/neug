include_directories(${CMAKE_CURRENT_BINARY_DIR}/../utils)
include_directories(${Boost_INCLUDE_DIRS})
add_executable(bulk_loader bulk_loader.cc)
add_executable(cypher_client cypher_client.cc)
target_link_libraries(cypher_client neug_utils neug_plan_proto ${ARROW_LIB} ${Boost_LIBRARIES} ${GLOG_LIBRARIES} ${GFLAGS_LIBRARIES} ${Protobuf_LIBRARIES} ${YAML_CPP_LIBRARIES})

if (BUILD_HTTP_SERVER)
        include_directories(../engines/http_server)
        add_executable(rt_server rt_server.cc)
        if (HTTP_SERVER_TYPE STREQUAL "hiactor")
                message(STATUS "Building rt_server with Hiactor HTTP server support")
                target_link_libraries(rt_server PUBLIC Hiactor::hiactor)
        else()
                message(STATUS "openssl library: ${OPENSSL_LIBRARIES}")
                set(MACOS_DYLIBS 
                        "-framework CoreFoundation"
                        "-framework CoreGraphics"
                        "-framework CoreData"
                        "-framework CoreText"
                        "-framework Security"
                        "-framework Foundation"
                        "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
                        "-Wl,-U,_ProfilerStart"
                        "-Wl,-U,_ProfilerStop"
                        "-Wl,-U,__Z13GetStackTracePPvii"
                        "-Wl,-U,_mallctl"
                        "-Wl,-U,_malloc_stats_print")
                target_link_libraries(rt_server PUBLIC ${BRPC_LIB} ${LEVELDB_LIB} ${OPENSSL_LIBRARIES} ${GFLAGS_LIBRARIES} ${GLOG_LIBRARIES} ${Protobuf_LIBRARIES} ${PROTOC_LIB} ${Boost_LIBRARIES})
                if (APPLE)
                        target_link_libraries(rt_server PUBLIC ${MACOS_DYLIBS})
                endif() 
        endif()
        if (BUILD_ALL_IN_ONE)
                target_link_libraries(rt_server PUBLIC neug_graph_db_service neug_libraries)
        else()
                target_link_libraries(rt_server PUBLIC neug_rt_mutable_graph neug_utils)
        endif()
        install_without_export_neug_target(rt_server)
endif()

if (BUILD_ALL_IN_ONE)
        target_link_libraries(bulk_loader neug_libraries ${GLOG_LIBRARIES} ${Boost_LIBRARIES} ${GFLAGS_LIBRARIES})
else()
        target_link_libraries(bulk_loader neug_rt_mutable_graph ${GLOG_LIBRARIES} ${Boost_LIBRARIES} ${GFLAGS_LIBRARIES})
endif()  

if (BUILD_WITH_OSS)
        target_link_libraries(bulk_loader cpp-sdk)
endif()

install_without_export_neug_target(bulk_loader)