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


function (build_glog_as_third_party)
    set(CMAKE_DEBUG_POSTFIX "" FORCE)
    set(WITH_GFLAGS OFF CACHE BOOL "Build glog without gflags" FORCE)
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build static library" FORCE)
    set(BUILD_TESTING OFF CACHE BOOL "Build glog tests" FORCE)

    # Disable libunwind in glog to prevent _Unwind_* symbol conflict with
    # libgcc_s.so.1. When both libunwind.so.8 and libgcc_s.so.1 are loaded,
    # the dynamic linker may resolve _Unwind_RaiseException to libunwind's
    # version which cannot dispatch C++ exceptions, causing abort() instead
    # of reaching catch blocks. With WITH_UNWIND=OFF, glog falls back to
    # _Unwind_Backtrace from <unwind.h> (provided by libgcc_s) for stack
    # traces in LOG(FATAL) / signal handlers — same quality, no conflict.
    set(WITH_UNWIND OFF CACHE BOOL "Disable libunwind to avoid symbol conflict with libgcc_s" FORCE)

    add_subdirectory(third_party/glog)
    include_directories(third_party/glog/src)
    include_directories(${CMAKE_CURRENT_BINARY_DIR}/third_party/glog/) # For generated headers
    set_target_properties(glog PROPERTIES DEBUG_POSTFIX "")
    set(GLOG_LIBRARIES glog::glog PARENT_SCOPE)
    set(GLOG_LIB glog::glog PARENT_SCOPE)
    set(GLOG_INCLUDE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/third_party/glog/src PARENT_SCOPE)
endfunction()
