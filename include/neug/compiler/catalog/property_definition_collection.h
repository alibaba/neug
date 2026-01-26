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

#include "neug/compiler/binder/ddl/property_definition.h"
#include "neug/compiler/common/case_insensitive_map.h"

namespace neug {
namespace catalog {

class NEUG_API PropertyDefinitionCollection {
 public:
  PropertyDefinitionCollection() : nextColumnID{0}, nextPropertyID{0} {}
  explicit PropertyDefinitionCollection(common::column_id_t nextColumnID)
      : nextColumnID{nextColumnID}, nextPropertyID{0} {}
  EXPLICIT_COPY_DEFAULT_MOVE(PropertyDefinitionCollection);

  common::idx_t size() const { return definitions.size(); }

  bool contains(const std::string& name) const {
    return nameToPropertyIDMap.contains(name);
  }

  std::vector<binder::PropertyDefinition> getDefinitions() const;
  const binder::PropertyDefinition& getDefinition(
      const std::string& name) const;
  const binder::PropertyDefinition& getDefinition(common::idx_t idx) const;
  common::column_id_t getMaxColumnID() const;
  common::column_id_t getColumnID(const std::string& name) const;
  common::column_id_t getColumnID(common::property_id_t propertyID) const;
  common::property_id_t getPropertyID(const std::string& name) const;
  void vacuumColumnIDs(common::column_id_t nextColumnID);

  void add(const binder::PropertyDefinition& definition);
  void drop(const std::string& name);
  void rename(const std::string& name, const std::string& newName);

  std::string toCypher() const;

  void serialize(common::Serializer& serializer) const;
  static PropertyDefinitionCollection deserialize(
      common::Deserializer& deserializer);

 private:
  PropertyDefinitionCollection(const PropertyDefinitionCollection& other)
      : nextColumnID{other.nextColumnID},
        nextPropertyID{other.nextPropertyID},
        definitions{copyMap(other.definitions)},
        columnIDs{other.columnIDs},
        nameToPropertyIDMap{other.nameToPropertyIDMap} {}

 private:
  common::column_id_t nextColumnID;
  common::property_id_t nextPropertyID;
  std::map<common::property_id_t, binder::PropertyDefinition> definitions;
  std::unordered_map<common::property_id_t, common::column_id_t> columnIDs;
  common::case_insensitive_map_t<common::property_id_t> nameToPropertyIDMap;
};

}  // namespace catalog
}  // namespace neug
