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

# Reference: https://github.com/apache/iceberg-cpp/blob/main/cmake_modules/IcebergThirdpartyToolchain.cmake


function (build_protobuf_as_third_party)
    set(BUILD_SHARED_LIBS ON CACHE BOOL "Build shared libraries")
    set(protobuf_BUILD_TESTS OFF CACHE BOOL "Build protobuf tests")
    set(protobuf_PROTOC_EXE OFF CACHE BOOL "Disable protoc executable")
    set(protobuf_BUILD_PROTOC_BINARIES OFF CACHE BOOL "Disable protoc binaries")
    set(protobuf_BUILD_LIBPROTOC ON CACHE BOOL "Disable libprotoc")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wno-sign-compare -Wno-deprecated-declarations -Wno-attributes")
    # Apply third_party/protobuf.patch if it exists
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf.patch")
        execute_process(
            COMMAND git apply "${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf.patch"
            WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf"
            RESULT_VARIABLE patch_result
            ERROR_VARIABLE error_output
        )
    else()
        message(FATAL_ERROR "third_party/protobuf.patch not found. Please ensure it exists in the third_party directory.")
    endif()
    add_subdirectory(third_party/protobuf)
    set(Protobuf_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf/src PARENT_SCOPE)
    set(Protobuf_INCLUDE_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf/src PARENT_SCOPE)
    set(Protobuf_LIBRARIES protobuf::libprotobuf protobuf::libprotobuf-lite protobuf::libprotoc PARENT_SCOPE)
    set(PROTOC_LIB protobuf::protoc PARENT_SCOPE)
endfunction()