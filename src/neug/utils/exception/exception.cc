#include "neug/utils/exception/exception.h"

#ifdef NEUG_BACKTRACE
#include <cpptrace/cpptrace.hpp>
#endif

namespace gs {
namespace exception {

Exception::Exception(std::string msg)
    : exception(), exception_message_(std::move(msg)) {
#ifdef NEUG_BACKTRACE
  cpptrace::generate_trace(1 /*skip this function's frame*/).print();
#endif
}

}  // namespace exception
}  // namespace gs
