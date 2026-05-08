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