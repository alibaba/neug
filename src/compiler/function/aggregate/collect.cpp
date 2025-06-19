#include "src/include/function/aggregate_function.h"

using namespace gs::binder;
using namespace gs::common;
using namespace gs::storage;
using namespace gs::processor;

namespace gs {
namespace function {

struct CollectState : public AggregateState {
  CollectState() : AggregateState() {}
  uint32_t getStateSize() const override { return sizeof(*this); }
  void moveResultToVector(common::ValueVector* outputVector,
                          uint64_t pos) override;
};

void CollectState::moveResultToVector(common::ValueVector* outputVector,
                                      uint64_t pos) {}

static std::unique_ptr<AggregateState> initialize() {
  return std::make_unique<CollectState>();
}

static void initCollectStateIfNecessary(CollectState* state,
                                        InMemOverflowBuffer* overflowBuffer,
                                        LogicalType& dataType) {}

static void updateSingleValue(CollectState* state, ValueVector* input,
                              uint32_t pos, uint64_t multiplicity,
                              InMemOverflowBuffer* overflowBuffer) {}

static void updateAll(uint8_t* state_, ValueVector* input,
                      uint64_t multiplicity,
                      InMemOverflowBuffer* overflowBuffer) {}

static void updatePos(uint8_t* state_, ValueVector* input,
                      uint64_t multiplicity, uint32_t pos,
                      InMemOverflowBuffer* overflowBuffer) {
  auto state = reinterpret_cast<CollectState*>(state_);
  updateSingleValue(state, input, pos, multiplicity, overflowBuffer);
}

static void finalize(uint8_t* /*state_*/) {}

static void combine(uint8_t* state_, uint8_t* otherState_,
                    InMemOverflowBuffer* /*overflowBuffer*/) {}

static std::unique_ptr<FunctionBindData> bindFunc(
    const ScalarBindFuncInput& input) {
  KU_ASSERT(input.arguments.size() == 1);
  auto aggFuncDefinition =
      reinterpret_cast<AggregateFunction*>(input.definition);
  aggFuncDefinition->parameterTypeIDs[0] =
      input.arguments[0]->dataType.getLogicalTypeID();
  auto returnType = LogicalType::LIST(input.arguments[0]->dataType.copy());
  return std::make_unique<FunctionBindData>(std::move(returnType));
}

function_set CollectFunction::getFunctionSet() {
  function_set result;
  for (auto isDistinct : std::vector<bool>{true, false}) {
    result.push_back(std::make_unique<AggregateFunction>(
        name, std::vector<LogicalTypeID>{LogicalTypeID::ANY},
        LogicalTypeID::LIST, initialize, updateAll, updatePos, combine,
        finalize, isDistinct, bindFunc, nullptr /* paramRewriteFunc */));
  }
  return result;
}

}  // namespace function
}  // namespace gs
