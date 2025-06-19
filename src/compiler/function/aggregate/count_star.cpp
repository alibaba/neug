#include "src/include/function/aggregate/count_star.h"

using namespace gs::common;
using namespace gs::storage;

namespace gs {
namespace function {

void CountStarFunction::updateAll(uint8_t* state_, ValueVector* input,
                                  uint64_t multiplicity,
                                  InMemOverflowBuffer* /*overflowBuffer*/) {
  auto state = reinterpret_cast<CountState*>(state_);
  KU_ASSERT(input == nullptr);
  (void) input;
  state->count += multiplicity;
}

void CountStarFunction::updatePos(uint8_t* state_, ValueVector* input,
                                  uint64_t multiplicity, uint32_t /*pos*/,
                                  InMemOverflowBuffer* /*overflowBuffer*/) {
  auto state = reinterpret_cast<CountState*>(state_);
  KU_ASSERT(input == nullptr);
  (void) input;
  state->count += multiplicity;
}

function_set CountStarFunction::getFunctionSet() {
  function_set result;
  result.push_back(std::make_unique<AggregateFunction>(
      name, std::vector<LogicalTypeID>{}, LogicalTypeID::INT64, initialize,
      updateAll, updatePos, combine, finalize, false));
  return result;
}

}  // namespace function
}  // namespace gs
