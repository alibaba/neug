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
#include <csignal>
#include <iostream>
#include <string>

#include <glog/logging.h>
#include "neug/main/file_lock.h"
#include "neug/utils/exception/exception.h"
#include "py_connection.h"
#include "py_database.h"
#include "py_query_result.h"

#define STRINGIFY(x) #x
#define MACRO_STRINGIFY(x) STRINGIFY(x)

namespace py = pybind11;

namespace gs {

void signal_handler(int signal) {
  LOG(INFO) << "Received signal " << signal << ", exiting...";
  // support SIGKILL, SIGINT, SIGTERM
  if (signal == SIGKILL || signal == SIGINT || signal == SIGTERM ||
      signal == SIGSEGV || signal == SIGABRT) {
    LOG(ERROR) << "Received signal " << signal << ", Remove all filelocks";
    // remove all files in work_dir
    gs::FileLock::CleanupAllLocks();
#ifdef NEUG_BACKTRACE
    cpptrace::generate_trace(1 /*skip this function's frame*/).print();
#endif
    exit(signal);
  } else {
    LOG(ERROR) << "Received unexpected signal " << signal << ", exiting...";
    exit(1);
  }
}

void setup_signal_handler() {
  // Register handlers for SIGKILL, SIGINT, SIGTERM, SIGSEGV, SIGABRT
  // LOG(FATAL) cause SIGABRT
  std::signal(SIGINT, signal_handler);
  std::signal(SIGTERM, signal_handler);
  std::signal(SIGKILL, signal_handler);
  std::signal(SIGSEGV, signal_handler);
  std::signal(SIGABRT, signal_handler);
  std::signal(SIGFPE, signal_handler);
  std::signal(SIGQUIT, signal_handler);
  std::signal(SIGKILL, signal_handler);
  std::signal(SIGHUP, signal_handler);
}
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
}  // namespace gs

PYBIND11_MODULE(neug_py_bind, m) {
  m.doc() = R"pbdoc(
        
        -----------------------
        GraphScope NeuG, a high performence embedded graph database.
        .. currentmodule:: neug

        .. autosummary::
           :toctree: _generate

    )pbdoc";

  m.attr("__version__") = MACRO_STRINGIFY(NEUG_VERSION);
  gs::PyDatabase::initialize(m);
  gs::PyConnection::initialize(m);
  gs::PyQueryResult::initialize(m);

  // Setup signal handling, for cleaning up resources on exit.
  gs::setup_signal_handler();

  // Register exception translation for Python.
  PYBIND11_CONSTINIT static py::gil_safe_call_once_and_store<py::object>
      exc_storage;
  exc_storage.call_once_and_store_result([&]() {
    return py::exception<gs::exception::Exception>(m, "Exception");
  });
  pybind11::register_exception_translator([](std::exception_ptr p) {
    try {
      if (p) {
        std::rethrow_exception(p);
      }
    } catch (const gs::exception::Exception& e) {
      pybind11::set_error(PyExc_RuntimeError, e.what());
    } catch (const std::exception& e) {
      pybind11::set_error(PyExc_RuntimeError, e.what());
    } catch (...) {
      pybind11::set_error(PyExc_RuntimeError, "Unknown exception");
    }
  });

  gs::setup_logging();
}
