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

function(build_brpc_as_third_party)
    set(CMAKE_DEBUG_POSTFIX "")
    set(BUILD_BRPC_AS_THIRD_PARTY TRUE CACHE BOOL "Build brpc as third party")
    message(STATUS "Fail to find brpc, try build from source")
    set(WITH_DEBUG_SYMBOLS OFF CACHE BOOL "Build brpc with debug symbols")
    set(BUILD_BRPC_TOOLS OFF CACHE BOOL "Build brpc tools")
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build brpc as static library" FORCE)
    set(WITH_GLOG ON CACHE BOOL "Build brpc with glog")
    set(BUILD_UNIT_TESTS OFF CACHE BOOL "Build brpc unit tests" FORCE)
    
    # We will always apply brpc_cmake.patch to fix the issue of dlsym issue related to packing wheel. 
    # https://github.com/alibaba/neug/issues/365
    execute_process(COMMAND git apply ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc_cmake.patch
                WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc
                RESULT_VARIABLE result
                OUTPUT_VARIABLE output
                ERROR_VARIABLE error_output)

    # Guard brpc's unconditional `#define HAVE_DLADDR` in build_config.h.
    # Python 3.14+ defines `#define HAVE_DLADDR 1` in pyconfig.h.  brpc's bare
    # `#define HAVE_DLADDR` (no value) is a different replacement list, so the
    # compiler emits a macro-redefinition warning that -Werror promotes to an error.
    # We unconditionally add the #ifndef guard since it's harmless if HAVE_DLADDR
    # is not already defined.
    set(_brpc_build_config "${CMAKE_CURRENT_SOURCE_DIR}/third_party/brpc/src/butil/build_config.h")
    file(READ "${_brpc_build_config}" _build_config_content)
    string(FIND "${_build_config_content}" "#ifndef HAVE_DLADDR" _has_guard)
    if(_has_guard EQUAL -1)
        string(REPLACE
            "#define HAVE_DLADDR"
            "#ifndef HAVE_DLADDR\n#define HAVE_DLADDR\n#endif"
            _build_config_content "${_build_config_content}")
        file(WRITE "${_brpc_build_config}" "${_build_config_content}")
        message(STATUS "Patched brpc build_config.h: added #ifndef guard for HAVE_DLADDR")
    endif()

    add_subdirectory(third_party/brpc)
    
    # Apply warning suppression flags to brpc targets
    if (TARGET brpc-static)
        target_compile_options(brpc-static PRIVATE -Wno-deprecated-declarations -Wno-nonnull -DDYNAMIC_ANNOTATIONS_ENABLED=0)
    endif()
    if (TARGET SOURCES_LIB)
        target_compile_options(SOURCES_LIB PRIVATE -Wno-deprecated-declarations -Wno-nonnull -DDYNAMIC_ANNOTATIONS_ENABLED=0)
    endif()
    if (TARGET BUTIL_LIB)
        message(STATUS "Applying warning suppression flags to BUTIL_LIB")
        target_compile_options(BUTIL_LIB PRIVATE -Wno-deprecated-declarations -Wno-nonnull -DDYNAMIC_ANNOTATIONS_ENABLED=0)
    endif()
    if (TARGET PROTO_LIB)
        message(STATUS "Applying warning suppression flags to PROTO_LIB")
        target_compile_options(PROTO_LIB PRIVATE -Wno-deprecated-declarations -Wno-nonnull -DDYNAMIC_ANNOTATIONS_ENABLED=0)
    endif()
    
    set(BRPC_LIB brpc-static PARENT_SCOPE)
    set(BRPC_ALL_LIBS brpc-static BUTIL_LIB PROTO_LIB)
    foreach(lib ${BRPC_ALL_LIBS})
        add_dependencies(${lib} ${Protobuf_LIBRARIES})
        if (TARGET protobuf::libprotobuf)
            add_dependencies(${lib}
                protobuf::libprotobuf
                protobuf::libprotobuf-lite
                protobuf::libprotoc)
        endif()
        if (TARGET protobuf::protoc)
            add_dependencies(${lib} protobuf::protoc)
        endif()
    endforeach()
    include_directories(${CMAKE_CURRENT_BINARY_DIR}/third_party/brpc/output/include)
endfunction(build_brpc_as_third_party)

