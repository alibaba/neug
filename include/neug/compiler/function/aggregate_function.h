/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include <functional>
#include <utility>

#include "neug/compiler/common/in_mem_overflow_buffer.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/function.h"

namespace neug {
namespace function {

struct AggregateState {
  virtual uint32_t getStateSize() const = 0;
  virtual void moveResultToVector(common::ValueVector* outputVector,
                                  uint64_t pos) = 0;
  virtual ~AggregateState() = default;

  bool isNull = true;
};

using param_rewrite_function_t =
    std::function<void(binder::expression_vector&)>;
using aggr_initialize_function_t =
    std::function<std::unique_ptr<AggregateState>()>;
using aggr_update_all_function_t = std::function<void(
    uint8_t* state, common::ValueVector* input, uint64_t multiplicity,
    common::InMemOverflowBuffer* overflowBuffer)>;
using aggr_update_pos_function_t = std::function<void(
    uint8_t* state, common::ValueVector* input, uint64_t multiplicity,
    uint32_t pos, common::InMemOverflowBuffer* overflowBuffer)>;
using aggr_combine_function_t =
    std::function<void(uint8_t* state, uint8_t* otherState,
                       common::InMemOverflowBuffer* overflowBuffer)>;
using aggr_finalize_function_t = std::function<void(uint8_t* state)>;

struct AggregateFunction final : public ScalarOrAggregateFunction {
  bool isDistinct;
  aggr_initialize_function_t initializeFunc;
  aggr_update_all_function_t updateAllFunc;
  aggr_update_pos_function_t updatePosFunc;
  aggr_combine_function_t combineFunc;
  aggr_finalize_function_t finalizeFunc;
  std::unique_ptr<AggregateState> initialNullAggregateState;
  // Rewrite aggregate on NODE/REL, e.g. COUNT(a) -> COUNT(a._id)
  param_rewrite_function_t paramRewriteFunc;

  AggregateFunction(std::string name,
                    std::vector<common::LogicalTypeID> parameterTypeIDs,
                    common::LogicalTypeID returnTypeID,
                    aggr_initialize_function_t initializeFunc,
                    aggr_update_all_function_t updateAllFunc,
                    aggr_update_pos_function_t updatePosFunc,
                    aggr_combine_function_t combineFunc,
                    aggr_finalize_function_t finalizeFunc, bool isDistinct,
                    scalar_bind_func bindFunc = nullptr,
                    param_rewrite_function_t paramRewriteFunc = nullptr)
      : ScalarOrAggregateFunction{std::move(name), std::move(parameterTypeIDs),
                                  returnTypeID, std::move(bindFunc)},
        isDistinct{isDistinct},
        initializeFunc{std::move(initializeFunc)},
        updateAllFunc{std::move(updateAllFunc)},
        updatePosFunc{std::move(updatePosFunc)},
        combineFunc{std::move(combineFunc)},
        finalizeFunc{std::move(finalizeFunc)},
        paramRewriteFunc{std::move(paramRewriteFunc)} {
    initialNullAggregateState = createInitialNullAggregateState();
  }

  EXPLICIT_COPY_DEFAULT_MOVE(AggregateFunction);

  common::idx_t getAggregateStateSize() const {
    return initialNullAggregateState->getStateSize();
  }

  // NOLINTNEXTLINE(readability-make-member-function-const): Returns a non-const
  // pointer.
  AggregateState* getInitialNullAggregateState() {
    return initialNullAggregateState.get();
  }

  std::unique_ptr<AggregateState> createInitialNullAggregateState() const {
    return initializeFunc();
  }

  void updateAllState(uint8_t* state, common::ValueVector* input,
                      uint64_t multiplicity,
                      common::InMemOverflowBuffer* overflowBuffer) const {
    return updateAllFunc(state, input, multiplicity, overflowBuffer);
  }

  void updatePosState(uint8_t* state, common::ValueVector* input,
                      uint64_t multiplicity, uint32_t pos,
                      common::InMemOverflowBuffer* overflowBuffer) const {
    return updatePosFunc(state, input, multiplicity, pos, overflowBuffer);
  }

  void combineState(uint8_t* state, uint8_t* otherState,
                    common::InMemOverflowBuffer* overflowBuffer) const {
    return combineFunc(state, otherState, overflowBuffer);
  }

  void finalizeState(uint8_t* state) const { return finalizeFunc(state); }

  bool isFunctionDistinct() const { return isDistinct; }

 private:
  AggregateFunction(const AggregateFunction& other);
};

struct AggregateFunctionUtils {
  template <typename T>
  static std::unique_ptr<AggregateFunction> getAggFunc(
      std::string name, common::LogicalTypeID inputType,
      common::LogicalTypeID resultType, bool isDistinct,
      param_rewrite_function_t paramRewriteFunc = nullptr) {
    return std::make_unique<AggregateFunction>(
        std::move(name), std::vector<common::LogicalTypeID>{inputType},
        resultType, T::initialize, T::updateAll, T::updatePos, T::combine,
        T::finalize, isDistinct, nullptr /* bindFunc */, paramRewriteFunc);
  }

  template <template <typename, typename> class FunctionType>
  static void appendSumOrAvgFuncs(std::string name,
                                  common::LogicalTypeID inputType,
                                  function_set& result);
};

struct AggregateSumFunction {
  static constexpr const char* name = "SUM";

  static function_set getFunctionSet();
};

struct AggregateAvgFunction {
  static constexpr const char* name = "AVG";

  static function_set getFunctionSet();
};

struct AggregateMinFunction {
  static constexpr const char* name = "MIN";

  static function_set getFunctionSet();
};

struct AggregateMaxFunction {
  static constexpr const char* name = "MAX";

  static function_set getFunctionSet();
};

struct CollectFunction {
  static constexpr const char* name = "COLLECT";

  static function_set getFunctionSet();
};

}  // namespace function
}  // namespace neug
