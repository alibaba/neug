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

#include "neug/execution/common/property_definition.h"

#include "neug/compiler/common/serializer/deserializer.h"
#include "neug/compiler/common/serializer/serializer.h"
#include "neug/utils/property/default_value.h"

using namespace neug::common;

namespace neug {

void PropertyDefinition::serialize(Serializer& serializer) const {
  serializer.serializeValue(columnDefinition.name);
  serializer.serializeValue(static_cast<uint8_t>(columnDefinition.type.id()));
}

PropertyDefinition PropertyDefinition::deserialize(Deserializer& deserializer) {
  std::string name;
  deserializer.deserializeValue(name);
  uint8_t typeIdVal;
  deserializer.deserializeValue(typeIdVal);
  auto type = DataType(static_cast<DataTypeId>(typeIdVal));
  auto columnDefinition = ColumnDefinition(name, std::move(type));
  return PropertyDefinition(std::move(columnDefinition),
                            get_default_value(columnDefinition.type));
}

}  // namespace neug
