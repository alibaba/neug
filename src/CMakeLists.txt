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
add_library(neug SHARED ${ALL_OBJECT_FILES})
if (ENABLE_GCOV)
    target_link_options(neug PRIVATE --coverage)
endif()
target_link_libraries(neug PUBLIC OpenSSL::SSL OpenSSL::Crypto)
target_link_libraries(neug PRIVATE ${GFLAGS_LIBRARIES} ${YAML_CPP_LIBRARIES} ${COMPILER_LIBRARIES})
target_link_libraries(neug PUBLIC ${ARROW_LIB} ${GLOG_LIBRARIES})
target_link_libraries(neug PRIVATE ${Protobuf_LIBRARIES})
if (BUILD_HTTP_SERVER)
    target_link_libraries(neug PRIVATE ${BRPC_LIB} ${LEVELDB_LIB})
    target_link_libraries(neug PUBLIC ${CRYPTO_LIB}) #${PROTOC_LIB}
endif()
if (APPLE)
    target_link_libraries(neug PUBLIC ${MACOS_DYLIBS})
endif()
if (WITH_MIMALLOC)
    # It must be PUBLIC, otherwise there will be performance differences.
    target_link_libraries(neug PUBLIC mimalloc)
endif()
target_include_directories(neug PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_BINARY_DIR}/compiler>
    $<INSTALL_INTERFACE:include/neug/compiler>)
install_neug_target(neug)
if (ENABLE_BACKTRACES)
    target_link_libraries(neug PUBLIC cpptrace::cpptrace)
endif()

