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
    # Build protobuf statically but avoid leaking BUILD_SHARED_LIBS to the rest of the project.
    if(DEFINED BUILD_SHARED_LIBS)
        set(_neug_prev_build_shared_libs ${BUILD_SHARED_LIBS})
        set(_neug_restore_build_shared_libs TRUE)
    else()
        set(_neug_restore_build_shared_libs FALSE)
    endif()
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "Build shared libraries" FORCE)

    set(protobuf_BUILD_SHARED_LIBS OFF CACHE BOOL "Build protobuf shared libraries" FORCE)
    set(protobuf_BUILD_TESTS OFF CACHE BOOL "Build protobuf tests" FORCE)
    set(protobuf_BUILD_CONFORMANCE OFF CACHE BOOL "Build protobuf conformance tests")
    set(protobuf_BUILD_EXAMPLES OFF CACHE BOOL "Build protobuf examples")
    set(protobuf_BUILD_PROTOC_BINARIES ON CACHE BOOL "Build protoc binaries")
    set(protobuf_BUILD_LIBPROTOC ON CACHE BOOL "Build libprotoc")
    set(protobuf_BUILD_LIBUPB ON CACHE BOOL "Build libupb")
    set(protobuf_WITH_ZLIB ON CACHE BOOL "Build with zlib")
    set(protobuf_INSTALL ON CACHE BOOL "Install protobuf targets and config files")
    set(protobuf_USE_EXTERNAL_GTEST ON CACHE BOOL "Use external GTest targets" FORCE)
    # Force absl to skip exporting test helper targets so its installed config
    # file does not depend on GTest::gmock, which is not shipped in our runtime.
    set(ABSL_BUILD_TEST_HELPERS OFF CACHE BOOL "Build Abseil test helper libraries" FORCE)
    set(ABSL_USE_EXTERNAL_GOOGLETEST OFF CACHE BOOL "Use external GoogleTest targets" FORCE)
    set(ABSL_FIND_GOOGLETEST OFF CACHE BOOL "Call find_package(GTest) from Abseil" FORCE)
    set(protobuf_ABSL_PROVIDER "package" CACHE STRING "Provider of absl library")
    # Apply third_party/protobuf.patch if it exists (optional for newer versions)
    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf.patch")
        execute_process(
            COMMAND git apply --check "${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf.patch"
            WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf"
            RESULT_VARIABLE patch_check_result
            ERROR_VARIABLE error_output
        )
        if(patch_check_result EQUAL 0)
            execute_process(
                COMMAND git apply "${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf.patch"
                WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf"
                RESULT_VARIABLE patch_result
                ERROR_VARIABLE error_output
            )
        else()
            message(STATUS "protobuf.patch does not apply cleanly (likely already fixed upstream), skipping.")
        endif()
    endif()
    add_subdirectory(third_party/protobuf)
    # Suppress warnings in third-party protobuf code (especially with -Werror).
    # -Wno-missing-requires: protobuf v29.6 port.h uses C++20 concepts that
    # trigger this warning on GCC 13+.
    foreach(_proto_tgt libprotobuf libprotobuf-lite libprotoc ${protobuf_ABSL_USED_TARGETS})
        if(TARGET ${_proto_tgt})
            target_compile_options(${_proto_tgt} PRIVATE
                -Wno-sign-compare -Wno-deprecated-declarations -Wno-attributes -Wno-ignored-attributes
                $<$<CXX_COMPILER_ID:GNU>:-Wno-stringop-overflow -Wno-stringop-overread -Wno-missing-requires>
            )
        endif()
    endforeach()
    # libprotobuf and friends default to hidden visibility when built as
    # static libs, which prevents symbols from being re-exported if we embed
    # them into our own shared objects. Relax the visibility so downstream
    # binaries reuse the single runtime bundled inside libneug.
    foreach(_proto_vis_target libprotobuf libprotobuf-lite libprotoc)
        if(TARGET ${_proto_vis_target})
            set_target_properties(${_proto_vis_target} PROPERTIES
                C_VISIBILITY_PRESET default
                CXX_VISIBILITY_PRESET default
                VISIBILITY_INLINES_HIDDEN OFF)
        endif()
    endforeach()
    # for all abseil-cpp targets used by protobuf, also suppress warnings (especially with -Werror)
    set(_absl_targets ${protobuf_ABSL_USED_TARGETS})
    if(protobuf_BUILD_TESTS)
        list(APPEND _absl_targets ${protobuf_ABSL_USED_TEST_TARGETS})
    endif()
    list(REMOVE_DUPLICATES _absl_targets)
    message(STATUS "protobuf uses abseil-cpp targets: ${_absl_targets}")
    foreach(_absl_tgt IN LISTS _absl_targets)
        if(TARGET ${_absl_tgt})
            target_compile_options(${_absl_tgt} PRIVATE
                -Wno-sign-compare -Wno-deprecated-declarations -Wno-attributes -Wno-ignored-attributes
                $<$<CXX_COMPILER_ID:GNU>:-Wno-stringop-overflow -Wno-stringop-overread -Wno-missing-requires>
            )
        else()
            message(STATUS "Requested abseil target ${_absl_tgt} not built in this configuration, skipping warning suppression")
        endif()
    endforeach()
    # Protobuf v22+ headers #include abseil headers (e.g. absl/log/absl_log.h),
    # so consumers need both protobuf/src AND abseil-cpp on the include path.
    if(DEFINED NEUG_ABSL_SOURCE_DIR)
        set(_neug_absl_include "${NEUG_ABSL_SOURCE_DIR}")
    else()
        set(_neug_absl_include "${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf/third_party/abseil-cpp")
    endif()
    set(_protobuf_include_dirs
        ${CMAKE_CURRENT_SOURCE_DIR}/third_party/protobuf/src
        ${_neug_absl_include}
    )
    list(REMOVE_DUPLICATES _protobuf_include_dirs)
    set(Protobuf_INCLUDE_DIRS ${_protobuf_include_dirs} PARENT_SCOPE)
    set(Protobuf_INCLUDE_DIR ${_protobuf_include_dirs} PARENT_SCOPE)
    set(Protobuf_LIBRARIES protobuf::libprotobuf protobuf::libprotobuf-lite protobuf::libprotoc PARENT_SCOPE)
    set(PROTOC_LIB protobuf::protoc PARENT_SCOPE)
    set(Protobuf_PROTOC_EXECUTABLE ${CMAKE_CURRENT_BINARY_DIR}/third_party/protobuf/protoc PARENT_SCOPE)

    if(_neug_restore_build_shared_libs)
        set(BUILD_SHARED_LIBS ${_neug_prev_build_shared_libs} CACHE BOOL "Build shared libraries" FORCE)
    else()
        unset(BUILD_SHARED_LIBS CACHE)
    endif()
endfunction()