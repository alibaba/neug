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

#include "neug/compiler/common/arrow/arrow_converter.h"

#include <cstring>

#include "neug/compiler/common/arrow/arrow_row_batch.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace common {

static void releaseArrowSchema(ArrowSchema* schema) {
  if (!schema || !schema->release) {
    return;
  }
  schema->release = nullptr;
  auto holder = static_cast<ArrowSchemaHolder*>(schema->private_data);
  delete holder;
}

// Copies the given string into the arrow holder's owned names and returns a
// pointer to the owned version
static const char* copyName(ArrowSchemaHolder& rootHolder,
                            const std::string& name) {
  auto length = name.length();
  std::unique_ptr<char[]> namePtr = std::make_unique<char[]>(length + 1);
  std::memcpy(namePtr.get(), name.c_str(), length);
  namePtr[length] = '\0';
  rootHolder.ownedTypeNames.push_back(std::move(namePtr));
  return rootHolder.ownedTypeNames.back().get();
}

void ArrowConverter::initializeChild(ArrowSchema& child,
                                     const std::string& name) {
  //! Child is cleaned up by parent
  child.private_data = nullptr;
  child.release = releaseArrowSchema;

  //! Store the child schema
  child.flags = ARROW_FLAG_NULLABLE;
  child.name = name.c_str();
  child.n_children = 0;
  child.children = nullptr;
  child.metadata = nullptr;
  child.dictionary = nullptr;
}

void ArrowConverter::setArrowFormatForStruct(ArrowSchemaHolder& rootHolder,
                                             ArrowSchema& child,
                                             const LogicalType& dataType) {
  child.format = "+s";
  // name is set by parent.
  child.n_children = (std::int64_t) StructType::getNumFields(dataType);
  rootHolder.nestedChildren.emplace_back();
  rootHolder.nestedChildren.back().resize(child.n_children);
  rootHolder.nestedChildrenPtr.emplace_back();
  rootHolder.nestedChildrenPtr.back().resize(child.n_children);
  for (auto i = 0u; i < child.n_children; i++) {
    rootHolder.nestedChildrenPtr.back()[i] =
        &rootHolder.nestedChildren.back()[i];
  }
  child.children = &rootHolder.nestedChildrenPtr.back()[0];
  for (auto i = 0u; i < child.n_children; i++) {
    initializeChild(*child.children[i]);
    const auto& structField = StructType::getField(dataType, i);
    child.children[i]->name = copyName(rootHolder, structField.getName());
    setArrowFormat(rootHolder, *child.children[i], structField.getType());
  }
}

void ArrowConverter::setArrowFormatForUnion(ArrowSchemaHolder& rootHolder,
                                            ArrowSchema& child,
                                            const LogicalType& dataType) {
  std::string formatStr = "+ud";
  child.n_children = (std::int64_t) UnionType::getNumFields(dataType);
  rootHolder.nestedChildren.emplace_back();
  rootHolder.nestedChildren.back().resize(child.n_children);
  rootHolder.nestedChildrenPtr.emplace_back();
  rootHolder.nestedChildrenPtr.back().resize(child.n_children);
  for (auto i = 0u; i < child.n_children; i++) {
    rootHolder.nestedChildrenPtr.back()[i] =
        &rootHolder.nestedChildren.back()[i];
  }
  child.children = &rootHolder.nestedChildrenPtr.back()[0];
  for (auto i = 0u; i < child.n_children; i++) {
    initializeChild(*child.children[i]);
    const auto& unionFieldType = UnionType::getFieldType(dataType, i);
    auto unionFieldName = UnionType::getFieldName(dataType, i);
    child.children[i]->name = copyName(rootHolder, unionFieldName);
    setArrowFormat(rootHolder, *child.children[i], unionFieldType);
    formatStr += (i == 0u ? ":" : ",") + std::to_string(i);
  }
  child.format = copyName(rootHolder, formatStr);
}

void ArrowConverter::setArrowFormatForInternalID(
    ArrowSchemaHolder& rootHolder, ArrowSchema& child,
    const LogicalType& /*dataType*/) {
  child.format = "+s";
  // name is set by parent.
  child.n_children = 2;
  rootHolder.nestedChildren.emplace_back();
  rootHolder.nestedChildren.back().resize(child.n_children);
  rootHolder.nestedChildrenPtr.emplace_back();
  rootHolder.nestedChildrenPtr.back().resize(child.n_children);
  for (auto i = 0u; i < child.n_children; i++) {
    rootHolder.nestedChildrenPtr.back()[i] =
        &rootHolder.nestedChildren.back()[i];
  }
  child.children = &rootHolder.nestedChildrenPtr.back()[0];
  initializeChild(*child.children[0]);
  child.children[0]->name = copyName(rootHolder, "offset");
  setArrowFormat(rootHolder, *child.children[0], LogicalType::INT64());
  initializeChild(*child.children[1]);
  child.children[1]->name = copyName(rootHolder, "table");
  setArrowFormat(rootHolder, *child.children[1], LogicalType::INT64());
}

void ArrowConverter::setArrowFormat(ArrowSchemaHolder& rootHolder,
                                    ArrowSchema& child,
                                    const LogicalType& dataType) {
  switch (dataType.getLogicalTypeID()) {
  case LogicalTypeID::BOOL: {
    child.format = "b";
  } break;
  case LogicalTypeID::INT128: {
    child.format = "d:38,0";
  } break;
  case LogicalTypeID::SERIAL:
  case LogicalTypeID::INT64: {
    child.format = "l";
  } break;
  case LogicalTypeID::INT32: {
    child.format = "i";
  } break;
  case LogicalTypeID::INT16: {
    child.format = "s";
  } break;
  case LogicalTypeID::INT8: {
    child.format = "c";
  } break;
  case LogicalTypeID::UINT64: {
    child.format = "L";
  } break;
  case LogicalTypeID::UINT32: {
    child.format = "I";
  } break;
  case LogicalTypeID::UINT16: {
    child.format = "S";
  } break;
  case LogicalTypeID::UINT8: {
    child.format = "C";
  } break;
  case LogicalTypeID::DOUBLE: {
    child.format = "g";
  } break;
  case LogicalTypeID::FLOAT: {
    child.format = "f";
  } break;
  case LogicalTypeID::DECIMAL: {
    auto formatString =
        "d:" + std::to_string(DecimalType::getPrecision(dataType)) + "," +
        std::to_string(DecimalType::getScale(dataType));
    child.format = copyName(rootHolder, formatString);
  } break;
  case LogicalTypeID::DATE: {
    child.format = "tdD";
  } break;
  case LogicalTypeID::TIMESTAMP_MS: {
    child.format = "tsm:";
  } break;
  case LogicalTypeID::TIMESTAMP_NS: {
    child.format = "tsn:";
  } break;
  case LogicalTypeID::TIMESTAMP_SEC: {
    child.format = "tss:";
  } break;
  case LogicalTypeID::TIMESTAMP_TZ: {
    auto format = "tsu:UTC";
    child.format = copyName(rootHolder, format);
  } break;
  case LogicalTypeID::TIMESTAMP: {
    child.format = "tsu:";
  } break;
  case LogicalTypeID::INTERVAL: {
    child.format = "tDu";
  } break;
  case LogicalTypeID::UUID:
  case LogicalTypeID::STRING: {
    child.format = "u";
  } break;
  case LogicalTypeID::BLOB: {
    child.format = "z";
  } break;
  case LogicalTypeID::LIST: {
    child.format = "+l";
    child.n_children = 1;
    rootHolder.nestedChildren.emplace_back();
    rootHolder.nestedChildren.back().resize(1);
    rootHolder.nestedChildrenPtr.emplace_back();
    rootHolder.nestedChildrenPtr.back().push_back(
        &rootHolder.nestedChildren.back()[0]);
    initializeChild(rootHolder.nestedChildren.back()[0]);
    child.children = &rootHolder.nestedChildrenPtr.back()[0];
    child.children[0]->name = "l";
    setArrowFormat(rootHolder, **child.children,
                   ListType::getChildType(dataType));
  } break;
  case LogicalTypeID::ARRAY: {
    auto numValuesPerArray =
        "+w:" + std::to_string(ArrayType::getNumElements(dataType));
    child.format = copyName(rootHolder, numValuesPerArray);
    child.n_children = 1;
    rootHolder.nestedChildren.emplace_back();
    rootHolder.nestedChildren.back().resize(1);
    rootHolder.nestedChildrenPtr.emplace_back();
    rootHolder.nestedChildrenPtr.back().push_back(
        &rootHolder.nestedChildren.back()[0]);
    initializeChild(rootHolder.nestedChildren.back()[0]);
    child.children = &rootHolder.nestedChildrenPtr.back()[0];
    child.children[0]->name = "l";
    setArrowFormat(rootHolder, **child.children,
                   ArrayType::getChildType(dataType));
  } break;
  case LogicalTypeID::MAP: {
    child.format = "+m";
    child.n_children = 1;
    rootHolder.nestedChildren.emplace_back();
    rootHolder.nestedChildren.back().resize(1);
    rootHolder.nestedChildrenPtr.emplace_back();
    rootHolder.nestedChildrenPtr.back().push_back(
        &rootHolder.nestedChildren.back()[0]);
    initializeChild(rootHolder.nestedChildren.back()[0]);
    child.children = &rootHolder.nestedChildrenPtr.back()[0];
    child.children[0]->name = "l";
    setArrowFormat(rootHolder, **child.children,
                   ListType::getChildType(dataType));
  } break;
  case LogicalTypeID::STRUCT:
  case LogicalTypeID::NODE:
  case LogicalTypeID::REL:
  case LogicalTypeID::RECURSIVE_REL:
    setArrowFormatForStruct(rootHolder, child, dataType);
    break;
  case LogicalTypeID::INTERNAL_ID:
    setArrowFormatForInternalID(rootHolder, child, dataType);
    break;
  case LogicalTypeID::UNION:
    setArrowFormatForUnion(rootHolder, child, dataType);
    break;
  default:
    THROW_RUNTIME_ERROR(
        stringFormat("{} cannot be exported to arrow.", dataType.toString()));
  }
}

std::unique_ptr<ArrowSchema> ArrowConverter::toArrowSchema(
    const std::vector<LogicalType>& dataTypes,
    const std::vector<std::string>& columnNames) {
  auto outSchema = std::make_unique<ArrowSchema>();
  auto rootHolder = std::make_unique<ArrowSchemaHolder>();

  auto columnCount = (int64_t) dataTypes.size();
  rootHolder->children.resize(columnCount);
  rootHolder->childrenPtrs.resize(columnCount);
  for (auto i = 0u; i < columnCount; i++) {
    rootHolder->childrenPtrs[i] = &rootHolder->children[i];
  }
  outSchema->children = rootHolder->childrenPtrs.data();
  outSchema->n_children = columnCount;

  outSchema->format = "+s";  // struct apparently
  outSchema->flags = 0;
  outSchema->metadata = nullptr;
  outSchema->name = "kuzu_query_result";
  outSchema->dictionary = nullptr;

  for (auto i = 0u; i < columnCount; i++) {
    auto& child = rootHolder->children[i];
    initializeChild(child);
    child.name = copyName(*rootHolder, columnNames[i]);
    setArrowFormat(*rootHolder, child, dataTypes[i]);
  }

  outSchema->private_data = rootHolder.release();
  outSchema->release = releaseArrowSchema;
  return outSchema;
}

}  // namespace common
}  // namespace neug
