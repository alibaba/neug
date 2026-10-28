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

# Apply platform-appropriate linker options to hide third-party symbols from a
# target's dynamic symbol table: protobuf/abseil (which would otherwise clash
# with pyarrow's bundled copies) and the statically linked OpenSSL (whose
# re-export lets a host that also exports OpenSSL -- notably Node.js, whose
# binary re-exports its own OpenSSL 3.x -- interpose libneug's bundled 1.1.1k
# on Linux's flat symbol namespace and segfault on the ABI mismatch; see #1159).
# Apply to every non-test binary that ships alongside libneug.so/dylib.
macro(neug_apply_symbol_visibility target)
    if(WIN32)
        # Windows: rely on the NEUG_API macro (defined in include/neug/utils/api.h)
        # to export only the public API. Exporting all symbols hits the linker
        # limit of 65535 exports (LNK1189) for a library of this size.
        set_target_properties(${target} PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS OFF)
    elseif(APPLE)
        target_link_options(${target} PRIVATE
            "LINKER:-unexported_symbols_list,${CMAKE_SOURCE_DIR}/cmake/neug_unexported.sym")
        set_target_properties(${target} PROPERTIES LINK_DEPENDS
            "${CMAKE_SOURCE_DIR}/cmake/neug_unexported.sym")
    else()
        target_link_options(${target} PRIVATE
            "LINKER:--version-script,${CMAKE_SOURCE_DIR}/cmake/neug_exports.ld"
            "LINKER:--exclude-libs,libprotobuf.a"
            "LINKER:--exclude-libs,libprotobuf-lite.a"
            "LINKER:--exclude-libs,libprotoc.a"
            "LINKER:--exclude-libs,libutf8_range.a"
            "LINKER:--exclude-libs,libutf8_validity.a"
            # OpenSSL is statically linked into libneug (bundled 1.1.1k). Keep
            # its SSL_*/EVP_*/CRYPTO_*/X509_* symbols local so a host process
            # that exports its own OpenSSL (e.g. Node.js 3.x) cannot interpose
            # them -- the version script's `global: *;` would otherwise
            # re-export them. See #1159.
            "LINKER:--exclude-libs,libssl.a"
            "LINKER:--exclude-libs,libcrypto.a"
        )
        set_target_properties(${target} PROPERTIES LINK_DEPENDS
            "${CMAKE_SOURCE_DIR}/cmake/neug_exports.ld")
    endif()
endmacro()
