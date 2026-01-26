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

#include "neug/compiler/common/assert.h"
#include "neug/compiler/parser/transaction_statement.h"
#include "neug/compiler/parser/transformer.h"

using namespace neug::transaction;
using namespace neug::common;

namespace neug {
namespace parser {

std::unique_ptr<Statement> Transformer::transformTransaction(
    CypherParser::NEUG_TransactionContext& ctx) {
  if (ctx.TRANSACTION()) {
    if (ctx.READ()) {
      return std::make_unique<TransactionStatement>(
          TransactionAction::BEGIN_READ);
    }
    return std::make_unique<TransactionStatement>(
        TransactionAction::BEGIN_WRITE);
  }
  if (ctx.COMMIT()) {
    return std::make_unique<TransactionStatement>(TransactionAction::COMMIT);
  }
  if (ctx.ROLLBACK()) {
    return std::make_unique<TransactionStatement>(TransactionAction::ROLLBACK);
  }
  if (ctx.CHECKPOINT()) {
    return std::make_unique<TransactionStatement>(
        TransactionAction::CHECKPOINT);
  }
  NEUG_UNREACHABLE;
}

}  // namespace parser
}  // namespace neug
