cmake_minimum_required (VERSION 3.10)
project (
  NeugTest
  VERSION 0.1
  LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

find_package(neug REQUIRED)
add_executable(test test.cc)
include_directories(${NEUG_INCLUDE_DIRS})
target_link_libraries(test ${NEUG_LIBRARIES})

# if ENV CI=ON, find arrow
if(DEFINED ENV{CI}) 
  if ("$ENV{CI}" STREQUAL "ON")
    find_package(Arrow CONFIG REQUIRED)
    message(STATUS "Found Arrow: ${ARROW_LIB}")
    foreach(_neug_arrow_target Arrow::arrow_static Arrow::arrow Arrow::arrow_shared)
        if(TARGET ${_neug_arrow_target})
            get_target_property(_neug_arrow_incs ${_neug_arrow_target} INTERFACE_INCLUDE_DIRECTORIES)
            message(STATUS "Arrow include dirs: ${_neug_arrow_incs}")
            include_directories(${_neug_arrow_incs})
            if(_neug_arrow_incs)
                set_property(TARGET test APPEND PROPERTY
                    INTERFACE_INCLUDE_DIRECTORIES "${_neug_arrow_incs}")
            endif()
            break()
        endif()
      endforeach()
      unset(_neug_arrow_target)
      unset(_neug_arrow_incs)
  endif()
endif()
