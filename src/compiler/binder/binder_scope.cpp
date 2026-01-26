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

#include "neug/compiler/binder/binder_scope.h"

namespace neug {
namespace binder {

void BinderScope::addExpression(const std::string& varName,
                                std::shared_ptr<Expression> expression) {
  nameToExprIdx.insert({varName, expressions.size()});
  expressions.push_back(std::move(expression));
}

void BinderScope::replaceExpression(const std::string& oldName,
                                    const std::string& newName,
                                    std::shared_ptr<Expression> expression) {
  NEUG_ASSERT(nameToExprIdx.contains(oldName));
  auto idx = nameToExprIdx.at(oldName);
  expressions[idx] = std::move(expression);
  nameToExprIdx.erase(oldName);
  nameToExprIdx.insert({newName, idx});
}

void BinderScope::clear() {
  expressions.clear();
  nameToExprIdx.clear();
}

}  // namespace binder
}  // namespace neug
