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
    set(GFLAGS_BUILD_SHARED_LIBS ON CACHE BOOL "Build shared libraries")
    set(GFLAGS_BUILD_STATIC_LIBS OFF CACHE BOOL "Build static libraries")
    set(GFLAGS_INSTALL_SHARED_LIBS ON CACHE BOOL "Install shared libraries")
    set(GFLAGS_INSTALL_STATIC_LIBS OFF CACHE BOOL "Install static libraries")
    set(GFLAGS_BUILD_gflags_LIB ON CACHE BOOL "Build gflags library")
    set(GFLAGS_BUILD_gflags_nothreads_LIB ON CACHE BOOL "Build gflags library")
    set(GFLAGS_USE_TARGET_NAMESPACE ON CACHE BOOL "Use target namespace for gflags")
    set(GFLAGS_INSTALL_HEADERS ON CACHE BOOL "Install gflags headers")
    set(GFLAGS_BUILD_TESTING OFF CACHE BOOL "Build gflags tests")
    set(GFLAGS_IS_SUBPROJECT ON )
    set(GFLAGS_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/third_party/gflags/src PARENT_SCOPE)
    set(GFLAGS_INCLUDE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/third_party/gflags/src PARENT_SCOPE)
    add_subdirectory(third_party/gflags)
    add_library(gflags ALIAS gflags_nothreads_shared)
    add_library(gflags::gflags ALIAS gflags_nothreads_shared)
    set(GFLAGS_LIBRARY gflags::gflags PARENT_SCOPE)
    set(GFLAGS_LIBRARIES gflags::gflags PARENT_SCOPE)
    set(gflags_FOUND TRUE PARENT_SCOPE)
    include_directories(SYSTEM ${CMAKE_CURRENT_BINARY_DIR}/third_party/gflags/include)
    include_directories(SYSTEM ${CMAKE_CURRENT_BINARY_DIR}/third_party/gflags/include/gflags/)
    if (TARGET gflags_nothreads_shared)
        set(GFLAGS_IS_SUBPROJECT TRUE PARENT_SCOPE)
    else()
        set(GFLAGS_IS_SUBPROJECT FALSE PARENT_SCOPE)
    endif()
endfunction()