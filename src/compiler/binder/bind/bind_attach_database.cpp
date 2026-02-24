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

#include "neug/compiler/binder/binder.h"
#include "neug/compiler/binder/bound_attach_database.h"
#include "neug/compiler/common/string_utils.h"
#include "neug/compiler/parser/attach_database.h"
#include "neug/compiler/parser/expression/parsed_literal_expression.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace binder {

std::unique_ptr<BoundStatement> Binder::bindAttachDatabase(
    const parser::Statement& statement) {
  auto& attachDatabase = statement.constCast<parser::AttachDatabase>();
  const auto& attachInfo = attachDatabase.getAttachInfo();

  binder::AttachOption attachOption;
  for (auto& [name, value] : attachInfo.options) {
    if (value->getExpressionType() != common::ExpressionType::LITERAL) {
      THROW_BINDER_EXCEPTION("Attach option must be a literal expression.");
    }
    auto val =
        value->constPtrCast<parser::ParsedLiteralExpression>()->getValue();
    attachOption.options.emplace(name, std::move(val));
  }

  if (common::StringUtils::getUpper(attachInfo.dbType) ==
          common::ATTACHED_NEUG_DB_TYPE &&
      attachInfo.dbAlias.empty()) {
    THROW_BINDER_EXCEPTION(
        "Attaching a gs database without an alias is not allowed.");
  }

  auto boundAttachInfo =
      binder::AttachInfo{attachInfo.dbPath, attachInfo.dbAlias,
                         attachInfo.dbType, std::move(attachOption)};
  return std::make_unique<BoundAttachDatabase>(std::move(boundAttachInfo));
}

}  // namespace binder
}  // namespace neug
