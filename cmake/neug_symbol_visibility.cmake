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

# Apply platform-appropriate linker options to hide Arrow, protobuf, and
# abseil symbols from a target's dynamic symbol table, preventing conflicts
# with pyarrow (which bundles its own copies of those libraries).
# Apply to every non-test binary that ships alongside libneug.so/dylib.
macro(neug_apply_symbol_visibility target)
    if(WIN32)
        message(FATAL_ERROR "neug_apply_symbol_visibility: symbol visibility control is not supported on Windows.")
    elseif(APPLE)
        target_link_options(${target} PRIVATE
            "LINKER:-unexported_symbols_list,${CMAKE_SOURCE_DIR}/cmake/neug_unexported.sym")
        set_target_properties(${target} PROPERTIES LINK_DEPENDS
            "${CMAKE_SOURCE_DIR}/cmake/neug_unexported.sym")
    else()
        target_link_options(${target} PRIVATE
            "LINKER:--version-script,${CMAKE_SOURCE_DIR}/cmake/neug_exports.ld"
            "LINKER:--exclude-libs,libarrow.a"
            "LINKER:--exclude-libs,libarrow_acero.a"
            "LINKER:--exclude-libs,libarrow_dataset.a"
            "LINKER:--exclude-libs,libarrow_bundled_dependencies.a"
            "LINKER:--exclude-libs,libparquet.a"
            "LINKER:--exclude-libs,libprotobuf.a"
            "LINKER:--exclude-libs,libprotobuf-lite.a"
            "LINKER:--exclude-libs,libprotoc.a"
            "LINKER:--exclude-libs,libutf8_range.a"
            "LINKER:--exclude-libs,libutf8_validity.a"
        )
        set_target_properties(${target} PROPERTIES LINK_DEPENDS
            "${CMAKE_SOURCE_DIR}/cmake/neug_exports.ld")
    endif()
endmacro()

# Variant for libneug.so itself: only hide Arrow/parquet symbols (to avoid
# clashing with pyarrow's bundled Arrow), but EXPORT protobuf/absl so that
# downstream executables (benchmark, rt_server) and neug_py_bind can share
# the SAME protobuf instance with libneug.so.  Having two protobuf instances
# in one process triggers either "Check failed: file != nullptr" (descriptors
# missing) or "File already exists in database" (double registration), both
# coming from generated_message_reflection.cc / descriptor.cc.
macro(neug_apply_lib_visibility target)
    if(WIN32)
        message(FATAL_ERROR "neug_apply_lib_visibility: symbol visibility control is not supported on Windows.")
    elseif(APPLE)
        # On macOS we still rely on the full unexported list; protobuf/absl in
        # libneug.dylib will be hidden which is acceptable because macOS isn't
        # the platform where benchmark/rt_server are typically run.
        target_link_options(${target} PRIVATE
            "LINKER:-unexported_symbols_list,${CMAKE_SOURCE_DIR}/cmake/neug_unexported.sym")
        set_target_properties(${target} PROPERTIES LINK_DEPENDS
            "${CMAKE_SOURCE_DIR}/cmake/neug_unexported.sym")
    else()
        # Hide ONLY Arrow/parquet symbols.  No version script, no protobuf hiding.
        target_link_options(${target} PRIVATE
            "LINKER:--exclude-libs,libarrow.a"
            "LINKER:--exclude-libs,libarrow_acero.a"
            "LINKER:--exclude-libs,libarrow_dataset.a"
            "LINKER:--exclude-libs,libarrow_bundled_dependencies.a"
            "LINKER:--exclude-libs,libparquet.a"
        )
    endif()
endmacro()