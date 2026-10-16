# bulk_loader is a POSIX-heavy utility executable; skip it on Windows when
# executables are disabled for the core build.
if(NOT WIN32 OR BUILD_EXECUTABLES)
    add_executable(bulk_loader bulk_loader.cc)
    target_link_libraries(bulk_loader PRIVATE neug ${GLOG_LIBRARIES})

    target_link_libraries(bulk_loader
        PRIVATE
        ${GFLAGS_LIBRARIES}
    )

    if (ENABLE_GCOV)
        target_compile_options(bulk_loader PRIVATE --coverage)
        target_link_options(bulk_loader PRIVATE --coverage)
    endif()

    install_without_export_neug_target(bulk_loader)
endif()
