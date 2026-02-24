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

#include "neug/compiler/binder/expression/expression.h"

#include "neug/compiler/function/function_signature_util.h"
#include "neug/utils/api.h"

namespace neug {

namespace main {
class ClientContext;
}

namespace function {

struct NEUG_API FunctionBindData {
  std::vector<common::LogicalType> paramTypes;
  common::LogicalType resultType;
  // TODO: the following two fields should be moved to FunctionLocalState.
  main::ClientContext* clientContext;
  int64_t count;

  explicit FunctionBindData(common::LogicalType dataType)
      : resultType{std::move(dataType)}, clientContext{nullptr}, count{1} {}
  FunctionBindData(std::vector<common::LogicalType> paramTypes,
                   common::LogicalType resultType)
      : paramTypes{std::move(paramTypes)},
        resultType{std::move(resultType)},
        clientContext{nullptr},
        count{1} {}
  DELETE_COPY_AND_MOVE(FunctionBindData);
  virtual ~FunctionBindData() = default;

  static std::unique_ptr<FunctionBindData> getSimpleBindData(
      const binder::expression_vector& params,
      const common::LogicalType& resultType);

  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }

  virtual std::unique_ptr<FunctionBindData> copy() const {
    return std::make_unique<FunctionBindData>(
        common::LogicalType::copy(paramTypes), resultType.copy());
  }
};

struct Function;
using function_set = std::vector<std::unique_ptr<Function>>;

struct ScalarBindFuncInput {
  const binder::expression_vector& arguments;
  Function* definition;
  main::ClientContext* context;
  std::vector<std::string> optionalArguments;

  ScalarBindFuncInput(const binder::expression_vector& arguments,
                      Function* definition, main::ClientContext* context,
                      std::vector<std::string> optionalArguments)
      : arguments{arguments},
        definition{definition},
        context{context},
        optionalArguments{std::move(optionalArguments)} {}
};

using scalar_bind_func = std::function<std::unique_ptr<FunctionBindData>(
    const ScalarBindFuncInput& bindInput)>;

struct NEUG_API Function {
  std::string name;
  std::vector<common::LogicalTypeID> parameterTypeIDs;
  std::string signatureName;
  // Currently we only one variable-length function which is list creation. The
  // expectation is that all parameters must have the same type as
  // parameterTypes[0]. For variable length function. A
  bool isVarLength = false;
  bool isListLambda = false;
  bool isReadOnly = true;

  Function() : isVarLength{false}, isListLambda{false}, isReadOnly{true} {};
  Function(std::string name,
           std::vector<common::LogicalTypeID> parameterTypeIDs)
      : name{std::move(name)},
        parameterTypeIDs{std::move(parameterTypeIDs)},
        isVarLength{false},
        isListLambda{false} {}
  Function(const Function&) = default;

  virtual ~Function() = default;

  virtual std::string signatureToString() const {
    return common::LogicalTypeUtils::toString(parameterTypeIDs);
  }

  void computeSignature() {
    this->signatureName = FunctionSignatureUtil::getSignatureName(
        this->name, this->parameterTypeIDs);
  }

  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }
  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }
};

struct ScalarOrAggregateFunction : Function {
  common::LogicalTypeID returnTypeID = common::LogicalTypeID::ANY;
  scalar_bind_func bindFunc = nullptr;

  ScalarOrAggregateFunction() : Function{} {}
  ScalarOrAggregateFunction(std::string name,
                            std::vector<common::LogicalTypeID> parameterTypeIDs,
                            common::LogicalTypeID returnTypeID)
      : Function{std::move(name), std::move(parameterTypeIDs)},
        returnTypeID{returnTypeID} {}
  ScalarOrAggregateFunction(std::string name,
                            std::vector<common::LogicalTypeID> parameterTypeIDs,
                            common::LogicalTypeID returnTypeID,
                            scalar_bind_func bindFunc)
      : Function{std::move(name), std::move(parameterTypeIDs)},
        returnTypeID{returnTypeID},
        bindFunc{std::move(bindFunc)} {}

  std::string signatureToString() const override {
    auto result = Function::signatureToString();
    result += " -> " + common::LogicalTypeUtils::toString(returnTypeID);
    return result;
  }
};

}  // namespace function
}  // namespace neug
