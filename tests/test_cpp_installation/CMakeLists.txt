cmake_minimum_required (VERSION 3.10)
project (
  NeugTest
  VERSION 0.1
  LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

# if ENV CI=ON, find arrow
if (DEFINED ENV{CI} AND $ENV{CI} STREQUAL "ON")
    find_package(Arrow REQUIRED)
    message(STATUS "Found Arrow: ${ARROW_LIB}")
endif()
find_package(neug REQUIRED)
add_executable(test test.cc)
include_directories(${NEUG_INCLUDE_DIRS})
target_link_libraries(test ${NEUG_LIBRARIES})
