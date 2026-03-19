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
#
# Build Abseil as a third-party dependency. Protobuf and other components
# rely on Abseil headers/libraries, so we vendor it explicitly to avoid
# sliding dependency chains.

function(_neug_strip_absl_randen_flags target)
    if(NOT TARGET ${target})
        return()
    endif()

    set(_neug_forbidden_patterns "-msse4\\.1" "-maes" "-Xarch_x86_64")
    foreach(_prop IN ITEMS COMPILE_OPTIONS INTERFACE_COMPILE_OPTIONS)
        get_target_property(_opts ${target} ${_prop})
        if(NOT _opts OR _opts STREQUAL "NOTFOUND")
            continue()
        endif()

        set(_filtered "")
        set(_changed FALSE)
        foreach(_opt IN LISTS _opts)
            set(_drop FALSE)
            foreach(_pattern IN LISTS _neug_forbidden_patterns)
                if(_opt MATCHES "${_pattern}")
                    set(_drop TRUE)
                    break()
                endif()
            endforeach()

            if(_drop)
                set(_changed TRUE)
            else()
                list(APPEND _filtered "${_opt}")
            endif()
        endforeach()

        if(_changed)
            if(_filtered)
                set_property(TARGET ${target} PROPERTY ${_prop} "${_filtered}")
            else()
                set_property(TARGET ${target} PROPERTY ${_prop} "")
            endif()
        endif()
    endforeach()
endfunction()

function(build_abseil_as_third_party)
    set(_absl_main "${CMAKE_CURRENT_SOURCE_DIR}/third_party/abseil-cpp")

    if(EXISTS "${_absl_main}/CMakeLists.txt")
        set(_absl_source "${_absl_main}")
        set(_absl_source_desc "top-level submodule")
    else()
        set(_absl_source "")
    endif()

    if(NOT _absl_source)
        message(FATAL_ERROR "Unable to locate Abseil sources. Run `git submodule update --init third_party/abseil-cpp` to fetch the submodule.")
    endif()

    set(NEUG_ABSL_SOURCE_DIR "${_absl_source}" PARENT_SCOPE)
    message(STATUS "Using Abseil from ${_absl_source_desc}: ${_absl_source}")

    if (TARGET absl::strings)
        message(STATUS "Abseil targets already available, reusing existing build.")
        if(DEFINED NEUG_ABSL_BINARY_DIR)
            set(absl_DIR "${NEUG_ABSL_BINARY_DIR}" CACHE PATH "Location of abslConfig.cmake" FORCE)
        endif()
        return()
    endif()

    if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/abseil-cpp.patch" AND EXISTS "${_absl_source}/.git")
        execute_process(
            COMMAND git apply --check "${CMAKE_CURRENT_SOURCE_DIR}/third_party/abseil-cpp.patch"
            WORKING_DIRECTORY "${_absl_source}"
            RESULT_VARIABLE _absl_patch_check
            ERROR_VARIABLE _absl_patch_error
        )
        if(_absl_patch_check EQUAL 0)
            execute_process(
                COMMAND git apply "${CMAKE_CURRENT_SOURCE_DIR}/third_party/abseil-cpp.patch"
                WORKING_DIRECTORY "${_absl_source}"
                RESULT_VARIABLE _absl_patch_result
                ERROR_VARIABLE _absl_patch_error
            )
            if(NOT _absl_patch_result EQUAL 0)
                message(FATAL_ERROR "Failed to apply abseil-cpp.patch: ${_absl_patch_error}")
            endif()
        else()
            message(STATUS "abseil-cpp.patch does not apply cleanly (likely already applied upstream), skipping.")
        endif()
    elseif(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/third_party/abseil-cpp.patch")
        message(WARNING "Skipping abseil-cpp.patch because ${_absl_source} is not a git checkout. Apply manually if needed.")
    endif()

    set(ABSL_PROPAGATE_CXX_STD ON CACHE BOOL "Propagate C++ standard to Abseil" FORCE)
    set(ABSL_BUILD_TESTING OFF CACHE BOOL "Build Abseil tests" FORCE)
    set(ABSL_ENABLE_INSTALL ON CACHE BOOL "Install Abseil alongside NeuG" FORCE)
    set(ABSL_USE_EXTERNAL_GOOGLETEST OFF CACHE BOOL "Use external gtest for Abseil" FORCE)

    set(_absl_binary_dir "${CMAKE_CURRENT_BINARY_DIR}/third_party/abseil-cpp")
    add_subdirectory("${_absl_source}" "${_absl_binary_dir}")

    # Apple Silicon toolchains reject x86-only flags such as -msse4.1, so drop
    # those options from Abseil's Randen targets when building arm64 wheels.
    if(APPLE)
        set(_neug_arch_candidates "")
        if(CMAKE_OSX_ARCHITECTURES)
            list(APPEND _neug_arch_candidates ${CMAKE_OSX_ARCHITECTURES})
        elseif(CMAKE_SYSTEM_PROCESSOR)
            list(APPEND _neug_arch_candidates ${CMAKE_SYSTEM_PROCESSOR})
        endif()

        set(_neug_has_arm64 FALSE)
        foreach(_arch IN LISTS _neug_arch_candidates)
            if(_arch MATCHES "arm64|aarch64")
                set(_neug_has_arm64 TRUE)
                break()
            endif()
        endforeach()

        if(_neug_has_arm64)
            foreach(_target IN ITEMS
                    absl_random_internal_randen_hwaes_impl
                    random_internal_randen_hwaes_impl
                    absl_random_internal_randen_hwaes
                    random_internal_randen_hwaes)
                _neug_strip_absl_randen_flags(${_target})
            endforeach()
        endif()
    endif()

    if(NOT TARGET absl::strings)
        message(FATAL_ERROR "Abseil targets missing after add_subdirectory. Verify Abseil checkout.")
    endif()

    set(absl_DIR "${_absl_binary_dir}" CACHE PATH "Location of abslConfig.cmake" FORCE)
    set(NEUG_ABSL_BINARY_DIR "${_absl_binary_dir}" CACHE PATH "Binary directory for Abseil" FORCE)
endfunction()
