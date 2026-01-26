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
#include <vector>

#include "neug/compiler/common/arrow/arrow.h"
#include "neug/compiler/common/arrow/arrow_nullmask_tree.h"
#include "neug/compiler/main/query_result.h"

struct ArrowSchema;

namespace neug {
namespace common {

struct ArrowSchemaHolder {
  std::vector<ArrowSchema> children;
  std::vector<ArrowSchema*> childrenPtrs;
  std::vector<std::vector<ArrowSchema>> nestedChildren;
  std::vector<std::vector<ArrowSchema*>> nestedChildrenPtr;
  std::vector<std::unique_ptr<char[]>> ownedTypeNames;
};

struct ArrowConverter {
 public:
  static std::unique_ptr<ArrowSchema> toArrowSchema(
      const std::vector<LogicalType>& dataTypes,
      const std::vector<std::string>& columnNames);

  static common::LogicalType fromArrowSchema(const ArrowSchema* schema);
  static void fromArrowArray(const ArrowSchema* schema, const ArrowArray* array,
                             ValueVector& outputVector, ArrowNullMaskTree* mask,
                             uint64_t srcOffset, uint64_t dstOffset,
                             uint64_t count);
  static void fromArrowArray(const ArrowSchema* schema, const ArrowArray* array,
                             ValueVector& outputVector);

 private:
  static void initializeChild(ArrowSchema& child, const std::string& name = "");
  static void setArrowFormatForStruct(ArrowSchemaHolder& rootHolder,
                                      ArrowSchema& child,
                                      const LogicalType& dataType);
  static void setArrowFormatForUnion(ArrowSchemaHolder& rootHolder,
                                     ArrowSchema& child,
                                     const LogicalType& dataType);
  static void setArrowFormatForInternalID(ArrowSchemaHolder& rootHolder,
                                          ArrowSchema& child,
                                          const LogicalType& dataType);
  static void setArrowFormat(ArrowSchemaHolder& rootHolder, ArrowSchema& child,
                             const LogicalType& dataType);
};

}  // namespace common
}  // namespace neug
