/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <pybind11/gil_safe_call_once.h>
#include <pybind11/pybind11.h>
#include <iostream>
#include <string>

#include <glog/logging.h>
#include "neug/utils/exception/exception.h"
#include "py_connection.h"
#include "py_database.h"
#include "py_query_request.h"
#include "py_query_result.h"
#ifdef NEUG_BACKTRACE
#include <cpptrace/cpptrace.hpp>
#endif
#ifdef NEUG_WITH_MIMALLOC
#include <mimalloc.h>
#include "neug/utils/mi_allocator.h"
#endif

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

namespace py = pybind11;

namespace neug {

#ifdef NEUG_WITH_MIMALLOC
// Tune mimalloc to minimize page faults under the explicit-allocator approach
// used in the Python binding (no global override / no LD_PRELOAD).
//
// Rationale: with explicit allocate_shared / vector_t / flat_hash_map_t, the
// process working set is split between glibc (Python/Arrow/numpy) and mimalloc
// (neug containers). Default mimalloc settings aggressively decommit/reset
// idle pages, causing extra minor page faults on re-touch. We disable that
// here, eagerly commit, and pre-warm the heap so segments are committed up
// front and stay resident.
static void tune_mimalloc_for_pybind() {
  // Read env overrides first; otherwise apply our defaults.
  //
  // Strategy: keep hot paths fault-free, but auto-return idle memory to OS
  // after a quiet period — no manual trim_memory() needed.
  //
  //   page_reset = 0          → do NOT MADV_FREE on every free; keeps pages
  //                              hot during high-frequency alloc/free loops.
  //   eager_commit = 1        → segment is fully committed when created so
  //                              first-touch faults amortize at warmup time.
  //   allow_decommit = 1      → allow decommit, BUT only after a delay…
  //   *_decommit_delay = 30s  → …so steady-state hot path never sees it; only
  //                              idle/quiescent segments are returned to OS.
  if (std::getenv("MIMALLOC_PAGE_RESET") == nullptr) {
    mi_option_set(mi_option_page_reset, 0);  // keep pages resident on free
  }
  if (std::getenv("MIMALLOC_EAGER_COMMIT") == nullptr) {
    mi_option_set(mi_option_eager_commit, 1);  // commit segment up front
  }
  // Auto-decommit idle segments after 30s of inactivity. This makes RSS
  // self-trim without any application-side calls.
  if (std::getenv("MIMALLOC_ALLOW_DECOMMIT") == nullptr) {
    mi_option_set(mi_option_allow_decommit, 1);
  }
  if (std::getenv("MIMALLOC_DECOMMIT_DELAY") == nullptr) {
    mi_option_set(mi_option_decommit_delay, 30000);  // ms
  }
  if (std::getenv("MIMALLOC_SEGMENT_DECOMMIT_DELAY") == nullptr) {
    mi_option_set(mi_option_segment_decommit_delay, 30000);  // ms
  }
  // Try to use 2 MiB *explicit* hugetlb OS pages (MAP_HUGETLB on Linux)
  if (std::getenv("MIMALLOC_LARGE_OS_PAGES") == nullptr) {
    mi_option_set(mi_option_large_os_pages, 1);
  }
}

#endif  // NEUG_WITH_MIMALLOC

void setup_logging() {
  google::InitGoogleLogging("neug");
  const char* debug = std::getenv("DEBUG");

  if (debug) {
    std::string mode = debug;
    if (mode == "1" || mode == "true" || mode == "ON") {
      FLAGS_minloglevel = 0;     // 0 for verbose, 1 for
      FLAGS_logtostderr = true;  // Log to stderr
    } else {
      std::cerr << "Invalid DEBUG value: " << mode
                << ". Expected '1', 'true', or 'ON'." << std::endl;
      FLAGS_minloglevel = 2;      // 2 for error, 3 for
      FLAGS_logtostderr = false;  // Log to file instead of stderr
    }
  } else {
    FLAGS_minloglevel = 2;      // 2 for error, 3 for fatal
    FLAGS_logtostderr = false;  // Log to file instead of stderr
  }
}
}  // namespace neug

PYBIND11_MODULE(neug_py_bind, m) {
  m.doc() = R"pbdoc(
        
        -----------------------
        GraphScope NeuG, a high performence embedded graph database.
        .. currentmodule:: neug

        .. autosummary::
           :toctree: _generate

    )pbdoc";

  m.attr("__version__") = MACRO_STRINGIFY(NEUG_VERSION);

#ifdef NEUG_WITH_MIMALLOC
  // Configure mimalloc *before* any neug type is registered so the first
  // container allocations already see the tuned options.
  neug::tune_mimalloc_for_pybind();
#endif

  neug::PyDatabase::initialize(m);
  neug::PyConnection::initialize(m);
  neug::PyQueryResult::initialize(m);
  neug::PyQueryRequest::initialize(m);

  // Register exception translation for Python.
  PYBIND11_CONSTINIT static py::gil_safe_call_once_and_store<py::object>
      exc_storage;
  exc_storage.call_once_and_store_result([&]() {
    return py::exception<neug::exception::Exception>(m, "Exception");
  });
  pybind11::register_exception_translator([](std::exception_ptr p) {
    try {
      if (p) {
        std::rethrow_exception(p);
      }
    } catch (const neug::exception::Exception& e) {
      pybind11::set_error(PyExc_RuntimeError, e.what());
    } catch (const std::exception& e) {
      pybind11::set_error(PyExc_RuntimeError, e.what());
    } catch (...) {
      pybind11::set_error(PyExc_RuntimeError, "Unknown exception");
    }
  });

  neug::setup_logging();
}
