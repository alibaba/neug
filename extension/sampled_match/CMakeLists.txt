# Find required packages for FaSTest
find_package(Boost REQUIRED)
find_package(GSL REQUIRED)

# Collect FaSTest source files (header-only library with some cpp files)
file(GLOB_RECURSE FASTEST_LIB_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/include/fastest_lib/src/**/*.cpp")

# Collect sampled_match extension source files
file(GLOB SAMPLED_MATCH_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

# Combine all sources
set(SAMPLED_MATCH_EXTENSION_OBJECT_FILES
    ${SAMPLED_MATCH_EXTENSION_SOURCES}
    ${FASTEST_LIB_SOURCES})

# Use the extension build system (should be called before add_extension_test)
build_extension_lib("sampled_match")

# Set include directories
target_include_directories(neug_sampled_match_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/fastest_lib/src
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Boost_INCLUDE_DIRS}
    ${GSL_INCLUDE_DIRS})

# Include rapidjson for JSON parsing
target_include_directories(neug_sampled_match_extension PRIVATE SYSTEM
    ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include)

# Link libraries
# protobuf::libprotobuf is added explicitly because src/CMakeLists.txt:35
# links protobuf as PRIVATE on the neug target — its INTERFACE_LINK_LIBRARIES
# does not propagate to downstream extension consumers, even though many neug
# PUBLIC headers (neug_db.h, connection.h, query_result.h, …) transitively
# include generated *.pb.h files. Linking it here ensures the extension can
# resolve protobuf symbols at link/load time independently of how libneug
# happens to expose (or not expose) them.
target_link_libraries(neug_sampled_match_extension PRIVATE
    ${GSL_LIBRARIES}
    Boost::boost
    neug
    protobuf::libprotobuf
)

# Runtime test: link only Neug + gtest; extension code runs exclusively from
# libsampled_match.neug_extension after LOAD. Embedding the same extension
# sources into the test binary and also dlopening the .so duplicates globals
# (e.g. GraphDataCache) and breaks RTTI across the DSO boundary, which showed
# up as a segfault right after CALL INITIALIZE() returned.
if(BUILD_TEST)
    set(_sm_test_name sampled_match_extension_test)
    find_package(OpenSSL REQUIRED)
    add_executable(${_sm_test_name}
        ${CMAKE_CURRENT_SOURCE_DIR}/tests/sampled_match_test.cpp)
    target_include_directories(${_sm_test_name} PRIVATE
        ${CMAKE_SOURCE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/include
        ${CMAKE_BINARY_DIR})
    if(ARROW_INCLUDE_DIRS)
        target_include_directories(${_sm_test_name} PRIVATE SYSTEM ${ARROW_INCLUDE_DIRS})
    endif()
    target_include_directories(${_sm_test_name} PRIVATE SYSTEM
        ${CMAKE_CURRENT_SOURCE_DIR}/include/fastest_lib/src
        ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include)
    target_link_libraries(${_sm_test_name} PUBLIC
        neug::GTest
        neug
        GSL::gsl
        GSL::gslcblas
        Boost::boost
        ${GLOG_LIBRARIES}
        ${ARROW_LIB}
        ${OPENSSL_LIBRARIES}
        ${CRYPTO_LIB})
    if(ENABLE_GCOV)
        target_compile_options(${_sm_test_name} PRIVATE --coverage)
        target_link_options(${_sm_test_name} PRIVATE --coverage)
    endif()
    include(GoogleTest)
    add_test(NAME ${_sm_test_name} COMMAND ${_sm_test_name})
    gtest_add_tests(TARGET ${_sm_test_name} TEST_LIST ${_sm_test_name}_tests)
    # Bundled gtest headers must beat Homebrew's -isystem gtest (see json tests).
    target_include_directories(${_sm_test_name} BEFORE PRIVATE
        ${CMAKE_BINARY_DIR}/_deps/googletest-src/googletest/include
        ${CMAKE_BINARY_DIR}/_deps/googletest-src/googlemock/include)
    add_dependencies(${_sm_test_name} neug_sampled_match_extension)
endif()
