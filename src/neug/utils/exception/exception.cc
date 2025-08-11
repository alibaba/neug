#include "neug/utils/exception/exception.h"

#ifdef NEUG_BACKTRACE
#include <cpptrace/cpptrace.hpp>
#endif

namespace gs {
namespace exception {

Exception::Exception(std::string msg, gs::StatusCode error_code)
    : exception(), exception_message_(std::move(msg)) {
#ifdef NEUG_BACKTRACE
  cpptrace::generate_trace(1 /*skip this function's frame*/).print();
#endif
  exception_message_ += ", Error Code: " + std::to_string(error_code);
}

Exception::Exception(std::string msg, std::string file_line,
                     gs::StatusCode error_code)
    : exception(), exception_message_(msg + " at " + file_line) {
#ifdef NEUG_BACKTRACE
  cpptrace::generate_trace(1 /*skip this function's frame*/).print();
#endif
  exception_message_ += ", Error Code: " + std::to_string(error_code);
}

}  // namespace exception
}  // namespace gs
