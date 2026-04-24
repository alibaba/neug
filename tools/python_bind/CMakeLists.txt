file(GLOB SOURCE_PY "neug/*")

file(GLOB_RECURSE SOURCE_CPP "${CMAKE_CURRENT_SOURCE_DIR}/src/*.cc")

# pybind11_add_module automatically enables LTO (via pybind11::lto) when
# CMAKE_INTERPROCEDURAL_OPTIMIZATION is not defined.  GCC-13's LTO
# partitioner crashes with an ICE (add_symbol_to_partition_1) when
# link-time-optimising this module together with the large set of statically
# linked libraries (Arrow, protobuf, abseil, …).  Explicitly set the
# variable to FALSE *before* the call so pybind11 skips its LTO logic.
set(CMAKE_INTERPROCEDURAL_OPTIMIZATION FALSE)
pybind11_add_module(neug_py_bind MODULE ${SOURCE_CPP})
include_directories(${CMAKE_CURRENT_BINARY_DIR}/../../src/compiler)
add_dependencies(neug_py_bind neug_plan_proto)
set(NEUG_LIBRARIES neug)
if (BUILD_HTTP_SERVER)
    set(NEUG_LIBRARIES ${NEUG_LIBRARIES})
endif()

# Link protobuf because pybind code uses protobuf-generated headers whose
# inline/template code references absl/protobuf symbols at link time.
# libneug.dylib hides these symbols to avoid conflicts with pyarrow, so
# neug_py_bind must link its own copy.
target_link_libraries(neug_py_bind PRIVATE ${NEUG_LIBRARIES} ${GLOG_LIBRARIES} ${Protobuf_LIBRARIES})

# Suppress VLA warnings from brpc third-party headers (bvar/detail/percentile.h
# uses DEFINE_SMALL_ARRAY which is a GCC VLA extension unsupported by Clang).
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_compile_options(neug_py_bind PRIVATE -Wno-vla-extension)
endif()

# Hide protobuf/abseil/arrow symbols from neug_py_bind.so as well, so they
# don't leak into the Python process and conflict with pyarrow's copies.
neug_apply_symbol_visibility(neug_py_bind)