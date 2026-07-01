add_executable(bulk_loader bulk_loader.cc)
target_link_libraries(bulk_loader PRIVATE neug ${GLOG_LIBRARIES})

find_package(OpenSSL REQUIRED)
target_link_libraries(bulk_loader
    PRIVATE
    ${OPENSSL_LIBRARIES}
    ${CRYPTO_LIB}
    ${GFLAGS_LIBRARIES}
)

# Under BUILD_HTTP_SERVER the neug headers included by bulk_loader compile in
# brpc/leveldb-referencing code paths, so the executable must link them to
# resolve those symbols at link time (neug links them PRIVATE, so they do not
# propagate to consumers).
if (BUILD_HTTP_SERVER)
    target_link_libraries(bulk_loader PRIVATE ${BRPC_LIB} ${LEVELDB_LIB})
endif()

if (ENABLE_GCOV)
    target_compile_options(bulk_loader PRIVATE --coverage)
    target_link_options(bulk_loader PRIVATE --coverage)
endif()

install_without_export_neug_target(bulk_loader)