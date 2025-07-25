#include "neug/compiler/function/date/date_functions.h"

#include "neug/compiler/main/client_context.h"

namespace gs {
namespace function {

void CurrentDate::operation(common::date_t& result, void* dataPtr) {
  auto currentTS = reinterpret_cast<FunctionBindData*>(dataPtr)
                       ->clientContext->getTransaction()
                       ->getCurrentTS();
  result = common::Timestamp::getDate(common::timestamp_tz_t(currentTS));
}

void CurrentTimestamp::operation(common::timestamp_tz_t& result,
                                 void* dataPtr) {
  auto currentTS = reinterpret_cast<FunctionBindData*>(dataPtr)
                       ->clientContext->getTransaction()
                       ->getCurrentTS();
  result = common::timestamp_tz_t(currentTS);
}

}  // namespace function
}  // namespace gs
