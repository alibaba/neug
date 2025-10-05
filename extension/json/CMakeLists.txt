file(GLOB_RECURSE JSON_SRC_FILES 
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/functions/*.cpp")

add_library(json SHARED ${JSON_SRC_FILES})
add_dependencies(json neug_plan_proto)

if(DEFINED ENV{HOME})
    set(USER_HOME $ENV{HOME})
else()
    set(USER_HOME "/tmp")
endif()

set_target_properties(json PROPERTIES
    OUTPUT_NAME "json"
    PREFIX "lib"
    SUFFIX ".neug_extension"
    LIBRARY_OUTPUT_DIRECTORY "${USER_HOME}/.neug/extensions/json"
)

message(STATUS "JSON extension will be built to: ${USER_HOME}/.neug/extensions/json")

target_include_directories(json
    PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/third_party
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(json
    PRIVATE
    ${ARROW_LIB}
    ${Protobuf_LIBRARIES}
    ${GLOG_LIBRARIES}
    neug_libraries
)

if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(json PRIVATE -fvisibility=default)
endif()


# --------- GoogleTest for extension only --------- 

option(JSON_EXTENSION_BUILD_TEST "Build json extension unit tests" ON)

if (JSON_EXTENSION_BUILD_TEST AND BUILD_TEST)

    set(_NEUG_CORE_TARGETS neug_libraries)

    foreach(tgt IN LISTS _NEUG_CORE_TARGETS)
        if (NOT TARGET ${tgt})
            message(FATAL_ERROR "[json extension] required core target '${tgt}' not found. Check add_subdirectory order.")
        endif()
    endforeach()

    if (NOT TARGET neug::GTest)
        message(FATAL_ERROR "[json extension] target neug::GTest not found. Make sure BUILD_TEST=ON at configure time.")
    endif()

    enable_testing()

    file(GLOB JSON_TEST_FILES "${CMAKE_CURRENT_SOURCE_DIR}/tests/*.cpp")

    add_executable(json_extension_test ${JSON_TEST_FILES})
    target_include_directories(json_extension_test
        PRIVATE
            ${CMAKE_SOURCE_DIR}/include
            ${CMAKE_SOURCE_DIR}/third_party
            ${CMAKE_CURRENT_SOURCE_DIR}/include
    )
    target_link_libraries(json_extension_test
        PRIVATE
            json
            ${_NEUG_CORE_TARGETS}
            neug::GTest
            ${Protobuf_LIBRARIES}
    )

    include(GoogleTest)
    gtest_discover_tests(json_extension_test)

    message(STATUS "[json extension] tests enabled. Core libs: ${_NEUG_CORE_TARGETS}")
endif()