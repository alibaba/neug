add_executable(bulk_loader bulk_loader.cc)

if (BUILD_ALL_IN_ONE)
        target_link_libraries(bulk_loader neug_libraries ${GLOG_LIBRARIES} ${Boost_LIBRARIES})
else()
        target_link_libraries(bulk_loader neug_storages ${GLOG_LIBRARIES} ${Boost_LIBRARIES})
endif() 

if (BUILD_WITH_OSS)
        target_link_libraries(bulk_loader cpp-sdk)
endif()

install_without_export_neug_target(bulk_loader)