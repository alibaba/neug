# Find required packages for FaSTest
find_package(Boost REQUIRED)
find_package(GSL REQUIRED)

# Collect FaSTest source files (header-only library with some cpp files)
file(GLOB_RECURSE FASTEST_LIB_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/include/FaSTest/src/**/*.cpp")

# Collect sampled_match extension source files
file(GLOB SAMPLED_MATCH_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

# Combine all sources
set(SAMPLED_MATCH_EXTENSION_OBJECT_FILES 
    ${SAMPLED_MATCH_EXTENSION_SOURCES}
    ${FASTEST_LIB_SOURCES})

# Use the extension build system
build_extension_lib("sampled_match")

if (${BUILD_TEST})
    add_subdirectory(test)
endif ()

# Set include directories
target_include_directories(neug_sampled_match_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/FaSTest/src
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Boost_INCLUDE_DIRS}
    ${GSL_INCLUDE_DIRS})

# Include rapidjson for JSON parsing
target_include_directories(neug_sampled_match_extension PRIVATE SYSTEM 
    ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include)

# Link libraries
target_link_libraries(neug_sampled_match_extension PRIVATE
    ${GSL_LIBRARIES}
    Boost::boost
    neug
)
