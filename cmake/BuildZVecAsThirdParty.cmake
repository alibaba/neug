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

include_guard(GLOBAL)

function(_neug_apply_patch source_dir patch_file patch_name)
    execute_process(
        COMMAND git rev-parse --show-toplevel
        WORKING_DIRECTORY "${source_dir}"
        RESULT_VARIABLE _git_check
        OUTPUT_VARIABLE _git_root
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET)
    file(REAL_PATH "${source_dir}" _source_real_path)
    if(_git_check EQUAL 0)
        file(REAL_PATH "${_git_root}" _git_real_path)
    endif()

    if(_git_check EQUAL 0 AND _git_real_path STREQUAL _source_real_path)
        set(_patch_check_command git apply --check "${patch_file}")
        set(_patch_apply_command git apply "${patch_file}")
        set(_patch_reverse_check_command
            git apply --reverse --check "${patch_file}")
    else()
        find_program(_patch_executable patch REQUIRED)
        set(_patch_check_command
            "${_patch_executable}" -p1 -f --dry-run -i "${patch_file}")
        set(_patch_apply_command
            "${_patch_executable}" -p1 -f -i "${patch_file}")
        set(_patch_reverse_check_command
            "${_patch_executable}" -p1 -f -R --dry-run -i "${patch_file}")
    endif()

    execute_process(
        COMMAND ${_patch_check_command}
        WORKING_DIRECTORY "${source_dir}"
        RESULT_VARIABLE _patch_check
        ERROR_VARIABLE _patch_error)
    if(_patch_check EQUAL 0)
        execute_process(
            COMMAND ${_patch_apply_command}
            WORKING_DIRECTORY "${source_dir}"
            RESULT_VARIABLE _patch_result
            ERROR_VARIABLE _patch_error)
        if(NOT _patch_result EQUAL 0)
            message(FATAL_ERROR
                "Failed to apply ${patch_name}: ${_patch_error}")
        endif()
        message(STATUS "Applied ${patch_name}.")
        return()
    endif()

    execute_process(
        COMMAND ${_patch_reverse_check_command}
        WORKING_DIRECTORY "${source_dir}"
        RESULT_VARIABLE _patch_applied
        ERROR_VARIABLE _patch_reverse_error)
    if(_patch_applied EQUAL 0)
        message(STATUS "${patch_name} is already applied.")
    else()
        message(FATAL_ERROR
            "${patch_name} neither applies nor appears already applied. "
            "Apply error: ${_patch_error} "
            "Reverse-check error: ${_patch_reverse_error}")
    endif()
endfunction()

function(_neug_apply_zvec_patch source_dir patch_file patch_name marker_name)
    if(NOT EXISTS "${source_dir}")
        message(FATAL_ERROR
            "${patch_name} source was not found at ${source_dir}. "
            "Initialize ZVec recursively with: git submodule update --init "
            "--recursive third_party/zvec")
    endif()
    if(NOT EXISTS "${patch_file}")
        message(FATAL_ERROR
            "${patch_name} file was not found at ${patch_file}.")
    endif()

    # ZVec's apply_patch_once() trusts its marker even if a later submodule
    # checkout reset the tracked files. Check the source itself, then recreate
    # the marker only after the patch is known to be present.
    _neug_apply_patch("${source_dir}" "${patch_file}" "${patch_name}")
    file(WRITE "${source_dir}/.${marker_name}_patched" "patched")
endfunction()

function(build_zvec_as_third_party)
    set(ZVEC_SOURCE_DIR
        "${CMAKE_SOURCE_DIR}/third_party/zvec"
        CACHE PATH
        "Path to the ZVec source tree")

    if(NOT EXISTS "${ZVEC_SOURCE_DIR}/CMakeLists.txt")
        message(FATAL_ERROR
            "ZVec source was not found at ${ZVEC_SOURCE_DIR}. "
            "Initialize it with: git submodule update --init --recursive "
            "third_party/zvec")
    endif()

    set(_zvec_patch "${CMAKE_SOURCE_DIR}/third_party/zvec.patch")
    if(EXISTS "${_zvec_patch}")
        _neug_apply_patch("${ZVEC_SOURCE_DIR}" "${_zvec_patch}" "zvec.patch")
    endif()

    _neug_apply_zvec_patch(
        "${ZVEC_SOURCE_DIR}/thirdparty/antlr/antlr4"
        "${ZVEC_SOURCE_DIR}/thirdparty/antlr/antlr4.patch"
        "ZVec ANTLR4 patch"
        "antlr4_fix")

    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        _neug_apply_zvec_patch(
            "${ZVEC_SOURCE_DIR}/thirdparty/glog/glog-0.5.0"
            "${ZVEC_SOURCE_DIR}/thirdparty/glog/glog.patch"
            "ZVec glog patch"
            "glog_fix")
        _neug_apply_zvec_patch(
            "${ZVEC_SOURCE_DIR}/thirdparty/arrow/apache-arrow-21.0.0"
            "${ZVEC_SOURCE_DIR}/thirdparty/arrow/arrow.patch"
            "ZVec Arrow patch"
            "arrow_fix")
    endif()

    include(ExternalProject)

    set(_zvec_binary_dir "${CMAKE_BINARY_DIR}/third_party/zvec-build")
    set(_zvec_roaring_include_dir
        "${ZVEC_SOURCE_DIR}/thirdparty/CRoaring/CRoaring-2.0.4")
    set(_zvec_roaring_library
        "${_zvec_binary_dir}/external/usr/local/lib/libroaring.a")
    if(APPLE)
        execute_process(
            COMMAND sysctl -n hw.physicalcpu
            OUTPUT_VARIABLE _zvec_parallel_jobs
            OUTPUT_STRIP_TRAILING_WHITESPACE)
    else()
        cmake_host_system_information(
            RESULT _zvec_parallel_jobs QUERY NUMBER_OF_PHYSICAL_CORES)
    endif()
    if(NOT _zvec_parallel_jobs OR _zvec_parallel_jobs LESS 1)
        set(_zvec_parallel_jobs 1)
    endif()

    set(_zvec_core_library "${_zvec_binary_dir}/lib/libzvec_core.a")
    set(_zvec_ailego_library "${_zvec_binary_dir}/lib/libzvec_ailego.a")
    set(_zvec_turbo_library "${_zvec_binary_dir}/lib/libzvec_turbo.a")

    # ZVec currently creates several common third-party targets unconditionally.
    # Build it in an isolated CMake project to avoid collisions with NeuG targets.
    set(_zvec_auto_detect_arch OFF)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux"
       AND CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|AMD64|x86|i[3-6]86)$")
        # ZVec's RaBitQ sources require their per-source AVX2 flags on x86.
        set(_zvec_auto_detect_arch ON)
    endif()

    set(_zvec_cmake_args
        "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
        "-DCMAKE_POLICY_VERSION_MINIMUM=3.5"
        "-DBUILD_ZVEC_CORE_SHARED=OFF"
        "-DBUILD_ZVEC_SHARED=OFF"
        "-DBUILD_ZVEC_AILEGO_SHARED=OFF"
        "-DBUILD_PYTHON_BINDINGS=OFF"
        "-DBUILD_C_BINDINGS=OFF"
        "-DAUTO_DETECT_ARCH=${_zvec_auto_detect_arch}"
        "-DBUILD_TOOLS=OFF")
    if(APPLE AND CMAKE_OSX_ARCHITECTURES)
        list(APPEND _zvec_cmake_args
            "-DCMAKE_OSX_ARCHITECTURES=${CMAKE_OSX_ARCHITECTURES}"
            "-DCMAKE_SYSTEM_PROCESSOR=${CMAKE_OSX_ARCHITECTURES}")
    endif()

    ExternalProject_Add(zvec_external
        SOURCE_DIR "${ZVEC_SOURCE_DIR}"
        BINARY_DIR "${_zvec_binary_dir}"
        INSTALL_COMMAND ""
        BUILD_COMMAND
            "${CMAKE_COMMAND}" --build <BINARY_DIR>
            --target zvec_core zvec_ailego zvec_turbo roaring
            --parallel ${_zvec_parallel_jobs}
        CMAKE_ARGS ${_zvec_cmake_args})

    add_library(zvec_core_static STATIC IMPORTED GLOBAL)
    set_target_properties(zvec_core_static PROPERTIES
        IMPORTED_LOCATION "${_zvec_core_library}"
        INTERFACE_INCLUDE_DIRECTORIES "${ZVEC_SOURCE_DIR}/src/include")
    add_dependencies(zvec_core_static zvec_external)

    add_library(zvec_ailego_static STATIC IMPORTED GLOBAL)
    set_target_properties(zvec_ailego_static PROPERTIES
        IMPORTED_LOCATION "${_zvec_ailego_library}")
    add_dependencies(zvec_ailego_static zvec_external)

    add_library(zvec_turbo_static STATIC IMPORTED GLOBAL)
    set_target_properties(zvec_turbo_static PROPERTIES
        IMPORTED_LOCATION "${_zvec_turbo_library}")
    add_dependencies(zvec_turbo_static zvec_external)

    add_library(zvec_roaring STATIC IMPORTED GLOBAL)
    set_target_properties(zvec_roaring PROPERTIES
        IMPORTED_LOCATION "${_zvec_roaring_library}"
        INTERFACE_INCLUDE_DIRECTORIES "${_zvec_roaring_include_dir}")
    add_dependencies(zvec_roaring zvec_external)

    set(ZVEC_SOURCE_DIR "${ZVEC_SOURCE_DIR}" PARENT_SCOPE)
endfunction()
