if(ENABLE_GCOV)
    add_compile_options(
        $<$<COMPILE_LANGUAGE:CXX>:--coverage>
        $<$<COMPILE_LANGUAGE:C>:--coverage>
    )
    add_link_options(--coverage)
    
    set(LCOV_EXCLUDE_PATTERNS 
        "*/third_party/*" 
        "*/external/*" 
        "/usr/include/*"
        CACHE STRING "Lcov exclude patterns")
endif()

add_subdirectory(utils)
add_subdirectory(storages)
add_subdirectory(transaction)
add_subdirectory(common)

if (BUILD_COMPILER)
    add_subdirectory(compiler)
    add_subdirectory(main)
endif()
add_subdirectory(server)
add_subdirectory(execution)

message(STATUS "Compiler libraries: ${COMPILER_LIBRARIES}")
add_library(neug_libraries SHARED ${ALL_OBJECT_FILES})
if (ENABLE_GCOV)
    target_link_options(neug_libraries PRIVATE --coverage)
endif()
target_link_libraries(neug_libraries PUBLIC ${Protobuf_LIBRARIES})
target_link_libraries(neug_libraries PRIVATE ${GFLAGS_LIBRARIES} ${GLOG_LIBRARIES} ${YAML_CPP_LIBRARIES} ${COMPILER_LIBRARIES} ${ARROW_LIB})
# Always link OpenSSL (needed by httplib in extension module)
find_package(OpenSSL REQUIRED)
target_link_libraries(neug_libraries PUBLIC OpenSSL::SSL OpenSSL::Crypto)
if (BUILD_HTTP_SERVER)
    target_link_libraries(neug_libraries PRIVATE ${BRPC_LIB} ${LEVELDB_LIB})
endif()
if (APPLE)
    target_link_libraries(neug_libraries PUBLIC ${MACOS_DYLIBS})
endif()
if (WITH_MIMALLOC)
    # It must be PUBLIC, otherwise there will be performance differences.
    target_link_libraries(neug_libraries PUBLIC mimalloc)
endif()
target_include_directories(neug_libraries PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/compiler>
    $<INSTALL_INTERFACE:include/neug/compiler>)
install_neug_target(neug_libraries)
if (ENABLE_BACKTRACES)
    target_link_libraries(neug_libraries PUBLIC cpptrace::cpptrace)
endif()
if (BUILD_HTTP_SERVER)
    find_package(absl QUIET)
    if (absl_FOUND AND TARGET absl::log_internal_message)
        # When protobuf is installed on macos with `brew install protobuf`, some absl symbols are not exposed correctly.
        # https://github.com/protocolbuffers/protobuf/issues/16314
        target_link_libraries(
            neug_libraries PRIVATE absl::log_internal_message absl::log_internal_check_op absl::status
        )
    endif()
endif()
