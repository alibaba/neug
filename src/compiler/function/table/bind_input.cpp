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

#include "neug/compiler/function/table/bind_input.h"

#include "neug/compiler/binder/expression/expression_util.h"
#include "neug/compiler/binder/expression/literal_expression.h"

namespace neug {
namespace function {

void TableFuncBindInput::addLiteralParam(common::Value value) {
  params.push_back(
      std::make_shared<binder::LiteralExpression>(std::move(value), ""));
}

common::Value TableFuncBindInput::getValue(common::idx_t idx) const {
  binder::ExpressionUtil::validateExpressionType(
      *params[idx], common::ExpressionType::LITERAL);
  return params[idx]->constCast<binder::LiteralExpression>().getValue();
}

template <typename T>
T TableFuncBindInput::getLiteralVal(common::idx_t idx) const {
  return getValue(idx).getValue<T>();
}

template NEUG_API std::string TableFuncBindInput::getLiteralVal<std::string>(
    common::idx_t idx) const;
template NEUG_API int64_t
TableFuncBindInput::getLiteralVal<int64_t>(common::idx_t idx) const;
template NEUG_API uint64_t
TableFuncBindInput::getLiteralVal<uint64_t>(common::idx_t idx) const;
template NEUG_API uint32_t
TableFuncBindInput::getLiteralVal<uint32_t>(common::idx_t idx) const;
template NEUG_API uint8_t* TableFuncBindInput::getLiteralVal<uint8_t*>(
    common::idx_t idx) const;

}  // namespace function
}  // namespace neug
