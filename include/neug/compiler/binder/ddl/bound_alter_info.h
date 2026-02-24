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
#include "neug/compiler/binder/expression/expression.h"
#include "neug/compiler/common/enums/alter_type.h"
#include "neug/compiler/common/enums/conflict_action.h"

namespace neug {
namespace binder {

struct BoundExtraAlterInfo {
  virtual ~BoundExtraAlterInfo() = default;

  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }
  template <class TARGET>
  const TARGET& constCast() const {
    return common::neug_dynamic_cast<const TARGET&>(*this);
  }
  template <class TARGET>
  TARGET& cast() {
    return common::neug_dynamic_cast<TARGET&>(*this);
  }

  virtual std::unique_ptr<BoundExtraAlterInfo> copy() const = 0;
};

struct BoundAlterInfo {
  common::AlterType alterType;
  std::string tableName;
  std::unique_ptr<BoundExtraAlterInfo> extraInfo;
  common::ConflictAction onConflict;

  BoundAlterInfo(common::AlterType alterType, std::string tableName,
                 std::unique_ptr<BoundExtraAlterInfo> extraInfo,
                 common::ConflictAction onConflict =
                     common::ConflictAction::ON_CONFLICT_THROW)
      : alterType{alterType},
        tableName{std::move(tableName)},
        extraInfo{std::move(extraInfo)},
        onConflict{onConflict} {}
  EXPLICIT_COPY_DEFAULT_MOVE(BoundAlterInfo);

  std::string toString() const;

 private:
  BoundAlterInfo(const BoundAlterInfo& other)
      : alterType{other.alterType},
        tableName{other.tableName},
        extraInfo{other.extraInfo->copy()},
        onConflict{other.onConflict} {}
};

struct BoundExtraRenameTableInfo final : BoundExtraAlterInfo {
  std::string newName;

  explicit BoundExtraRenameTableInfo(std::string newName)
      : newName{std::move(newName)} {}
  BoundExtraRenameTableInfo(const BoundExtraRenameTableInfo& other)
      : newName{other.newName} {}

  std::unique_ptr<BoundExtraAlterInfo> copy() const override {
    return std::make_unique<BoundExtraRenameTableInfo>(*this);
  }
};

struct BoundExtraAddPropertyInfo final : BoundExtraAlterInfo {
  PropertyDefinition propertyDefinition;
  std::shared_ptr<Expression> boundDefault;

  BoundExtraAddPropertyInfo(const PropertyDefinition& definition,
                            std::shared_ptr<Expression> boundDefault)
      : propertyDefinition{definition.copy()},
        boundDefault{std::move(boundDefault)} {}
  BoundExtraAddPropertyInfo(const BoundExtraAddPropertyInfo& other)
      : propertyDefinition{other.propertyDefinition.copy()},
        boundDefault{other.boundDefault} {}

  std::unique_ptr<BoundExtraAlterInfo> copy() const override {
    return std::make_unique<BoundExtraAddPropertyInfo>(*this);
  }
};

struct BoundExtraDropPropertyInfo final : BoundExtraAlterInfo {
  std::string propertyName;

  explicit BoundExtraDropPropertyInfo(std::string propertyName)
      : propertyName{std::move(propertyName)} {}
  BoundExtraDropPropertyInfo(const BoundExtraDropPropertyInfo& other)
      : propertyName{other.propertyName} {}

  std::unique_ptr<BoundExtraAlterInfo> copy() const override {
    return std::make_unique<BoundExtraDropPropertyInfo>(*this);
  }
};

struct BoundExtraRenamePropertyInfo final : BoundExtraAlterInfo {
  std::string newName;
  std::string oldName;

  BoundExtraRenamePropertyInfo(std::string newName, std::string oldName)
      : newName{std::move(newName)}, oldName{std::move(oldName)} {}
  BoundExtraRenamePropertyInfo(const BoundExtraRenamePropertyInfo& other)
      : newName{other.newName}, oldName{other.oldName} {}
  std::unique_ptr<BoundExtraAlterInfo> copy() const override {
    return std::make_unique<BoundExtraRenamePropertyInfo>(*this);
  }
};

struct BoundExtraCommentInfo final : BoundExtraAlterInfo {
  std::string comment;

  explicit BoundExtraCommentInfo(std::string comment)
      : comment{std::move(comment)} {}
  BoundExtraCommentInfo(const BoundExtraCommentInfo& other)
      : comment{other.comment} {}
  std::unique_ptr<BoundExtraAlterInfo> copy() const override {
    return std::make_unique<BoundExtraCommentInfo>(*this);
  }
};

}  // namespace binder
}  // namespace neug
