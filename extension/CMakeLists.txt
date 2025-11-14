message(STATUS "=== Building extensions: ${BUILD_EXTENSIONS} ===")

string(REPLACE ";" " " BUILD_EXTENSIONS_STR "${BUILD_EXTENSIONS}")
separate_arguments(EXTENSION_LIST UNIX_COMMAND "${BUILD_EXTENSIONS_STR}")

set(EXTENSION_INCLUDE_BASE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/include")
set(EXTENSION_COMMON_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/common")

file(GLOB_RECURSE EXTENSION_COMMON_SRC 
    "${EXTENSION_COMMON_SRC_DIR}/*.cpp"
)

if(DEFINED ENV{HOME})
    set(USER_HOME $ENV{HOME})
else()
    set(USER_HOME "/tmp")
endif()

foreach(EXT_NAME IN LISTS EXTENSION_LIST)
    string(STRIP "${EXT_NAME}" EXT_NAME)
    string(TOLOWER "${EXT_NAME}" EXT_NAME_LOWER)
    
    if(NOT EXT_NAME)
        continue()
    endif()
    
    message(STATUS "=== Configuring extension: ${EXT_NAME} ===")
    
    set(EXT_SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src/${EXT_NAME_LOWER}")
    
    if(NOT EXISTS "${EXT_SRC_DIR}")
        message(WARNING "Extension source directory not found: ${EXT_SRC_DIR}")
        continue()
    endif()
    
    file(GLOB_RECURSE EXT_SRC_FILES
        "${EXT_SRC_DIR}/*.cpp"
    )
    
    if(NOT EXT_SRC_FILES)
        message(WARNING "No source files found for extension: ${EXT_NAME}")
        continue()
    endif()
    
    set(ALL_SRC_FILES ${EXT_SRC_FILES})
    if(EXTENSION_COMMON_SRC)
        list(APPEND ALL_SRC_FILES ${EXTENSION_COMMON_SRC})
        message(STATUS "  Including ${CMAKE_CURRENT_LIST_LENGTH} common source files")
    endif()
    
    add_library(${EXT_NAME_LOWER} SHARED ${ALL_SRC_FILES})
    add_dependencies(${EXT_NAME_LOWER} neug_plan_proto)
    
    set_target_properties(${EXT_NAME_LOWER} PROPERTIES
        OUTPUT_NAME "${EXT_NAME_LOWER}"
        PREFIX "lib"
        SUFFIX ".neug_extension"
        LIBRARY_OUTPUT_DIRECTORY "${USER_HOME}/.neug/extensions/${EXT_NAME_LOWER}"
    )
    
    target_include_directories(${EXT_NAME_LOWER}
        PRIVATE
            ${EXTENSION_INCLUDE_BASE_DIR}      # extension/include (包含 common/ 和 json/ 等)
            ${CMAKE_SOURCE_DIR}/include        # neug/include
            ${CMAKE_SOURCE_DIR}/third_party    # third_party
    )
    
    target_link_libraries(${EXT_NAME_LOWER}
        PRIVATE
            neug_libraries
            ${ARROW_LIB}
            ${Protobuf_LIBRARIES}
            ${GLOG_LIBRARIES}
    )
    
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_compile_options(${EXT_NAME_LOWER} PRIVATE -fvisibility=default)
    endif()
    
    message(STATUS "  Output: ${USER_HOME}/.neug/extensions/${EXT_NAME_LOWER}/lib${EXT_NAME_LOWER}.neug_extension")
    
    # ========== Build Test ==========
    if(BUILD_TEST)
        set(EXT_TEST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/tests/${EXT_NAME_LOWER}")
        
        if(EXISTS "${EXT_TEST_DIR}")
            file(GLOB_RECURSE EXT_TEST_FILES "${EXT_TEST_DIR}/*.cpp")
            
            if(EXT_TEST_FILES)
                set(COMMON_TEST_DIR "${CMAKE_CURRENT_SOURCE_DIR}/tests/common")
                if(EXISTS "${COMMON_TEST_DIR}")
                    file(GLOB_RECURSE COMMON_TEST_FILES "${COMMON_TEST_DIR}/*.cpp")
                    if(COMMON_TEST_FILES)
                        list(APPEND EXT_TEST_FILES ${COMMON_TEST_FILES})
                        message(STATUS "  Including common test files")
                    endif()
                endif()
                
                add_executable(${EXT_NAME_LOWER}_extension_test ${EXT_TEST_FILES})
                
                target_include_directories(${EXT_NAME_LOWER}_extension_test
                    PRIVATE
                        ${EXTENSION_INCLUDE_BASE_DIR}
                        ${CMAKE_SOURCE_DIR}/include
                        ${CMAKE_SOURCE_DIR}/third_party
                )
                
                target_link_libraries(${EXT_NAME_LOWER}_extension_test
                    PRIVATE
                        ${EXT_NAME_LOWER}
                        ${GLOG_LIBRARIES}
                        neug_libraries
                        neug::GTest
                        ${Protobuf_LIBRARIES}
                        ${OPENSSL_LIBRARIES}
                        ${CRYPTO_LIB}
                        ${GFLAGS_LIBRARIES}
                )
                
                include(GoogleTest)
                gtest_discover_tests(${EXT_NAME_LOWER}_extension_test)
                
                message(STATUS "  Tests enabled for ${EXT_NAME} extension")
            endif()
        endif()
    endif()
    
endforeach()

message(STATUS "=== Extension configuration complete ===")