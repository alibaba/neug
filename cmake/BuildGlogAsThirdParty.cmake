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
    add_subdirectory(third_party/glog)
    include_directories(SYSTEM third_party/glog/src)
    include_directories(SYSTEM ${CMAKE_CURRENT_BINARY_DIR}/third_party/glog/) # For generated headers
    set_target_properties(glog PROPERTIES DEBUG_POSTFIX "")
    set(GLOG_LIBRARIES glog::glog PARENT_SCOPE)
    set(GLOG_LIB glog::glog PARENT_SCOPE)
    set(GLOG_INCLUDE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/third_party/glog/src PARENT_SCOPE)
endfunction()