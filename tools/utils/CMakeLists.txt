add_executable(bulk_loader bulk_loader.cc)
target_link_libraries(bulk_loader PRIVATE neug_libraries ${GLOG_LIBRARIES})


find_package(OpenSSL REQUIRED)
target_link_libraries(bulk_loader
    PRIVATE
    ${OPENSSL_LIBRARIES}
    ${CRYPTO_LIB}
    ${GFLAGS_LIBRARIES}
)

if (BUILD_WITH_OSS)
        target_link_libraries(bulk_loader PRIVATE cpp-sdk)
endif()

install_without_export_neug_target(bulk_loader)