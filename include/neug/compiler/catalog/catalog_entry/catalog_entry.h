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

#include <string>

#include "neug/compiler/catalog/catalog_entry/catalog_entry_type.h"
#include "neug/compiler/common/assert.h"
#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/serializer/deserializer.h"
#include "neug/compiler/common/serializer/serializer.h"
#include "neug/compiler/common/types/types.h"

namespace neug {
namespace main {
class ClientContext;
}  // namespace main

namespace catalog {

struct NEUG_API ToCypherInfo {
  virtual ~ToCypherInfo() = default;

  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }
};

class NEUG_API CatalogEntry {
 public:
  //===--------------------------------------------------------------------===//
  // constructor & destructor
  //===--------------------------------------------------------------------===//
  CatalogEntry() : CatalogEntry{CatalogEntryType::DUMMY_ENTRY, ""} {}
  CatalogEntry(CatalogEntryType type, std::string name)
      : type{type}, name{std::move(name)}, oid{common::INVALID_OID} {}
  DELETE_COPY_DEFAULT_MOVE(CatalogEntry);
  virtual ~CatalogEntry() = default;

  //===--------------------------------------------------------------------===//
  // getter & setter
  //===--------------------------------------------------------------------===//
  CatalogEntryType getType() const { return type; }
  void rename(std::string name_) { this->name = std::move(name_); }
  std::string getName() const { return name; }
  bool hasParent() const { return hasParent_; }
  void setHasParent(bool hasParent) { hasParent_ = hasParent; }
  void setOID(common::oid_t oid) { this->oid = oid; }
  common::oid_t getOID() const { return oid; }
  //===--------------------------------------------------------------------===//
  // serialization & deserialization
  //===--------------------------------------------------------------------===//
  virtual void serialize(common::Serializer& serializer) const;
  static std::unique_ptr<CatalogEntry> deserialize(
      common::Deserializer& deserializer);

  virtual std::string toCypher(const ToCypherInfo& /*info*/) const {
    NEUG_UNREACHABLE;
  }

  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }
  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }
  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }
  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }

 protected:
  virtual void copyFrom(const CatalogEntry& other);

 protected:
  CatalogEntryType type;
  std::string name;
  common::oid_t oid;
  bool hasParent_ = false;
};

}  // namespace catalog
}  // namespace neug
