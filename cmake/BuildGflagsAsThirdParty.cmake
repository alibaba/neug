#!/bin/bash
# Copyright 2020 Alibaba Group Holding Limited.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


function (build_gflags_as_third_party)
    set(CMAKE_DEBUG_POSTFIX "" FORCE)
    # Force using third_party gflags instead of system version
    # This avoids version conflicts and include path issues
    set(gflags_FOUND FALSE)
    if (NOT gflags_FOUND)
        set(GFLAGS_BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries" FORCE)
        set(GFLAGS_BUILD_STATIC_LIBS ON CACHE BOOL "Build static libraries" FORCE)
        set(GFLAGS_INSTALL_SHARED_LIBS OFF CACHE BOOL "Install shared libraries" FORCE)
        set(GFLAGS_INSTALL_STATIC_LIBS ON CACHE BOOL "Install static libraries" FORCE)
        set(GFLAGS_BUILD_gflags_LIB OFF CACHE BOOL "Build gflags library" FORCE)
        set(GFLAGS_BUILD_gflags_nothreads_LIB ON CACHE BOOL "Build gflags library" FORCE)
        set(GFLAGS_NAMESPACE "google" CACHE STRING "Namespace for gflags")
        set(GFLAGS_INSTALL_HEADERS OFF CACHE BOOL "Install gflags headers")
        set(INSTALL_HEADERS OFF CACHE BOOL "Install gflags headers")
        set(GFLAGS_BUILD_TESTING OFF CACHE BOOL "Build gflags tests")
        set(GFLAGS_IS_SUBPROJECT ON)
        set(BUILD_PACKAGING OFF CACHE BOOL "Build gflags packaging")
        set(GFLAGS_INCLUDE_DIRS ${CMAKE_CURRENT_BINARY_DIR}/third_party/gflags/include PARENT_SCOPE)
        set(GFLAGS_INCLUDE_PATH ${CMAKE_CURRENT_BINARY_DIR}/third_party/gflags/include PARENT_SCOPE)
        include_directories(SYSTEM ${GFLAGS_INCLUDE_PATH})

        execute_process(COMMAND git apply ${CMAKE_CURRENT_SOURCE_DIR}/third_party/gflags.patch
                    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/third_party/gflags
                    RESULT_VARIABLE result
                    OUTPUT_VARIABLE output
                    ERROR_VARIABLE error_output)
        add_subdirectory(third_party/gflags)

        set(GFLAGS_LIBRARY gflags::gflags PARENT_SCOPE)
        set(GFLAGS_LIBRARIES gflags::gflags PARENT_SCOPE)
        set(gflags_FOUND TRUE PARENT_SCOPE)
        set(GFLAGS_FOUND TRUE PARENT_SCOPE)
        set(GFLAGS_IS_SUBPROJECT TRUE PARENT_SCOPE)
    endif()

    # find glog---------------------------------------------------------------------
    # Force using third_party glog instead of system version
    set(GLOG_FOUND FALSE)
    if (NOT GLOG_FOUND)
        message(STATUS "Building glog from third_party")
        # Disable gflags in glog to avoid find_package conflicts during build
        # glog will still work, just without gflags command-line parsing
        set(WITH_GFLAGS OFF CACHE BOOL "Build glog without gflags" FORCE)
        set(BUILD_SHARED_LIBS ON CACHE BOOL "Build shared libraries" FORCE)
        set(BUILD_TESTING OFF CACHE BOOL "Build glog tests" FORCE)
        add_subdirectory(third_party/glog)
        include_directories(SYSTEM third_party/glog/src)
        include_directories(SYSTEM ${CMAKE_CURRENT_BINARY_DIR}/third_party/glog/) # For generated headers
        set_target_properties(glog PROPERTIES DEBUG_POSTFIX "")
        set(GLOG_LIBRARIES glog::glog PARENT_SCOPE)
        set(GLOG_LIB glog::glog PARENT_SCOPE)
        set(GLOG_INCLUDE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/third_party/glog/src PARENT_SCOPE)
        message(STATUS "glog libraries: ${GLOG_LIBRARIES}")
    else()
        include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})
        set(GLOG_LIBRARIES ${GLOG_LIBRARIES} PARENT_SCOPE)
    endif()
endfunction()