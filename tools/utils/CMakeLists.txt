add_executable(bulk_loader bulk_loader.cc)
target_link_libraries(bulk_loader neug_libraries ${GLOG_LIBRARIES})


if (BUILD_WITH_OSS)
        target_link_libraries(bulk_loader cpp-sdk)
endif()

install_without_export_neug_target(bulk_loader)