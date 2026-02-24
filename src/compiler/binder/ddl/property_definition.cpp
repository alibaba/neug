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

#include "neug/compiler/binder/ddl/property_definition.h"

#include "neug/compiler/common/serializer/deserializer.h"
#include "neug/compiler/common/serializer/serializer.h"
#include "neug/compiler/parser/expression/parsed_literal_expression.h"

using namespace neug::common;
using namespace neug::parser;

namespace neug {
namespace binder {

PropertyDefinition::PropertyDefinition(ColumnDefinition columnDefinition)
    : columnDefinition{std::move(columnDefinition)} {
  defaultExpr = std::make_unique<ParsedLiteralExpression>(
      Value::createNullValue(), "NULL");
}

void PropertyDefinition::serialize(Serializer& serializer) const {
  serializer.serializeValue(columnDefinition.name);
  columnDefinition.type.serialize(serializer);
  defaultExpr->serialize(serializer);
}

PropertyDefinition PropertyDefinition::deserialize(Deserializer& deserializer) {
  std::string name;
  deserializer.deserializeValue(name);
  auto type = LogicalType::deserialize(deserializer);
  auto columnDefinition = ColumnDefinition(name, std::move(type));
  auto defaultExpr = ParsedExpression::deserialize(deserializer);
  return PropertyDefinition(std::move(columnDefinition),
                            std::move(defaultExpr));
}

}  // namespace binder
}  // namespace neug
