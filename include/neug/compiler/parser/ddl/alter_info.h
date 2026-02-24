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

#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/enums/alter_type.h"
#include "neug/compiler/common/enums/conflict_action.h"
#include "neug/compiler/parser/expression/parsed_expression.h"

namespace neug {
namespace parser {

struct ExtraAlterInfo {
  virtual ~ExtraAlterInfo() = default;

  template <class TARGET>
  const TARGET* constPtrCast() const {
    return common::neug_dynamic_cast<const TARGET*>(this);
  }
  template <class TARGET>
  TARGET* ptrCast() {
    return common::neug_dynamic_cast<TARGET*>(this);
  }
};

struct AlterInfo {
  common::AlterType type;
  std::string tableName;
  std::unique_ptr<ExtraAlterInfo> extraInfo;
  common::ConflictAction onConflict;

  AlterInfo(common::AlterType type, std::string tableName,
            std::unique_ptr<ExtraAlterInfo> extraInfo,
            common::ConflictAction onConflict =
                common::ConflictAction::ON_CONFLICT_THROW)
      : type{type},
        tableName{std::move(tableName)},
        extraInfo{std::move(extraInfo)},
        onConflict{onConflict} {}
  DELETE_COPY_DEFAULT_MOVE(AlterInfo);
};

struct ExtraRenameTableInfo : public ExtraAlterInfo {
  std::string newName;

  explicit ExtraRenameTableInfo(std::string newName)
      : newName{std::move(newName)} {}
};

struct ExtraAddPropertyInfo : public ExtraAlterInfo {
  std::string propertyName;
  std::string dataType;
  std::unique_ptr<ParsedExpression> defaultValue;

  ExtraAddPropertyInfo(std::string propertyName, std::string dataType,
                       std::unique_ptr<ParsedExpression> defaultValue)
      : propertyName{std::move(propertyName)},
        dataType{std::move(dataType)},
        defaultValue{std::move(defaultValue)} {}
};

struct ExtraDropPropertyInfo : public ExtraAlterInfo {
  std::string propertyName;

  explicit ExtraDropPropertyInfo(std::string propertyName)
      : propertyName{std::move(propertyName)} {}
};

struct ExtraRenamePropertyInfo : public ExtraAlterInfo {
  std::string propertyName;
  std::string newName;

  ExtraRenamePropertyInfo(std::string propertyName, std::string newName)
      : propertyName{std::move(propertyName)}, newName{std::move(newName)} {}
};

struct ExtraCommentInfo : public ExtraAlterInfo {
  std::string comment;

  explicit ExtraCommentInfo(std::string comment)
      : comment{std::move(comment)} {}
};

}  // namespace parser
}  // namespace neug
