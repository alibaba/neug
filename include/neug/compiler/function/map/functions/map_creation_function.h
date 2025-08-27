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

#include "neug/compiler/common/type_utils.h"
#include "neug/compiler/common/types/value/value.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/list/functions/list_unique_function.h"
#include "neug/compiler/main/client_context.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace function {

static void duplicateValueHandler(const std::string& key) {
  THROW_RUNTIME_ERROR(
      common::stringFormat("Found duplicate key: {} in map.", key));
}

static void nullValueHandler() {
  THROW_RUNTIME_ERROR("Null value key is not allowed in map.");
}

static void validateKeys(common::list_entry_t& keyEntry,
                         common::ValueVector& keyVector) {
  ListUnique::appendListElementsToValueSet(
      keyEntry, keyVector, duplicateValueHandler,
      nullptr /* uniqueValueHandler */, nullValueHandler);
}

struct MapCreation {
  static void operation(common::list_entry_t& keyEntry,
                        common::list_entry_t& valueEntry,
                        common::list_entry_t& resultEntry,
                        common::ValueVector& keyVector,
                        common::ValueVector& valueVector,
                        common::ValueVector& resultVector, void* dataPtr) {
    if (keyEntry.size != valueEntry.size) {
      THROW_RUNTIME_ERROR("Unaligned key list and value list.");
    }
    if (!reinterpret_cast<FunctionBindData*>(dataPtr)
             ->clientContext->getClientConfig()
             ->disableMapKeyCheck) {
      validateKeys(keyEntry, keyVector);
    }
    resultEntry = common::ListVector::addList(&resultVector, keyEntry.size);
    auto resultStructVector = common::ListVector::getDataVector(&resultVector);
    copyListEntry(resultEntry,
                  common::StructVector::getFieldVector(resultStructVector,
                                                       0 /* keyVector */)
                      .get(),
                  keyEntry, &keyVector);
    copyListEntry(resultEntry,
                  common::StructVector::getFieldVector(resultStructVector,
                                                       1 /* valueVector */)
                      .get(),
                  valueEntry, &valueVector);
  }

  static void copyListEntry(common::list_entry_t& resultEntry,
                            common::ValueVector* resultVector,
                            common::list_entry_t& srcEntry,
                            common::ValueVector* srcVector) {
    auto resultPos = resultEntry.offset;
    auto srcDataVector = common::ListVector::getDataVector(srcVector);
    auto srcPos = srcEntry.offset;
    for (auto i = 0u; i < srcEntry.size; i++) {
      resultVector->copyFromVectorData(resultPos++, srcDataVector, srcPos++);
    }
  }
};

}  // namespace function
}  // namespace gs
