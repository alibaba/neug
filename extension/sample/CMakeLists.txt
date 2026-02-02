# Find required packages for FaSTest
find_package(Boost REQUIRED)
find_package(GSL REQUIRED)

# Collect FaSTest source files (header-only library with some cpp files)
file(GLOB_RECURSE FASTEST_LIB_SOURCES 
    "${CMAKE_CURRENT_SOURCE_DIR}/include/FaSTest/lib/**/*.cpp")

# Collect sample extension source files
file(GLOB SAMPLE_EXTENSION_SOURCES
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc"
    "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp")

# Combine all sources
set(SAMPLE_EXTENSION_OBJECT_FILES 
    ${SAMPLE_EXTENSION_SOURCES}
    ${FASTEST_LIB_SOURCES})

# Use the extension build system
build_extension_lib("sample")

if (${BUILD_TEST})
    add_subdirectory(test)
endif ()

# Set include directories
target_include_directories(neug_sample_extension PRIVATE
    ${CMAKE_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/FaSTest/lib
    ${CMAKE_CURRENT_BINARY_DIR}
    ${Boost_INCLUDE_DIRS}
    ${GSL_INCLUDE_DIRS})

# Include rapidjson for JSON parsing
target_include_directories(neug_sample_extension PRIVATE SYSTEM 
    ${CMAKE_SOURCE_DIR}/third_party/rapidjson/include)

# Link libraries
target_link_libraries(neug_sample_extension PRIVATE
    ${Protobuf_LIBRARIES}
    ${GLOG_LIBRARIES}
    ${GSL_LIBRARIES}
    Boost::boost
    neug_libraries
)
