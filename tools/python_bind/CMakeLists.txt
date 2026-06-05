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
add_dependencies(neug_py_bind neug_proto)

# BUILD_RPATH covers both layouts because setup.py copies the build-tree .so
# straight into the wheel (no cmake --install), so INSTALL_RPATH never gets
# baked in for wheel artifacts:
#   - dev:   <build>/tools/python_bind/neug_py_bind*.so → ../../src/libneug.*
#   - wheel: <site-packages>/neug_py_bind*.so          → sibling libneug.*
# Root CMake omits $ORIGIN on Linux+Clang, so we set it explicitly here.
if(APPLE)
    set(_neug_py_build_rpath "@loader_path/../../src;@loader_path")
    set(_neug_py_install_rpath "@loader_path")
else()
    set(_neug_py_build_rpath "$ORIGIN/../../src;$ORIGIN")
    set(_neug_py_install_rpath "$ORIGIN")
endif()
set_target_properties(neug_py_bind PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/tools/python_bind"
    BUILD_RPATH "${_neug_py_build_rpath}"
    INSTALL_RPATH "${_neug_py_install_rpath}"
    BUILD_WITH_INSTALL_RPATH FALSE)

# protobuf is linked privately because pybind sources use protobuf-generated
# headers whose inline/template code references absl/protobuf symbols;
# libneug.dylib hides those symbols to avoid conflicts with pyarrow.
target_link_libraries(neug_py_bind PRIVATE neug ${GLOG_LIBRARIES} ${Protobuf_LIBRARIES})

# Suppress VLA warnings from brpc third-party headers (bvar/detail/percentile.h
# uses DEFINE_SMALL_ARRAY which is a GCC VLA extension unsupported by Clang).
if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    target_compile_options(neug_py_bind PRIVATE -Wno-vla-extension)
endif()

# Hide protobuf/abseil/arrow symbols from neug_py_bind.so as well, so they
# don't leak into the Python process and conflict with pyarrow's copies.
neug_apply_symbol_visibility(neug_py_bind)