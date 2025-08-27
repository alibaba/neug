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

#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/vector/value_vector.h"
#include "neug/compiler/function/hash/hash_functions.h"

namespace gs {
namespace function {

static bool isAllInternalIDDistinct(
    common::ValueVector* dataVector, common::offset_t startOffset,
    uint64_t size,
    std::unordered_set<common::internalID_t, InternalIDHasher>& internalIDSet) {
  internalIDSet.clear();
  for (auto i = 0u; i < size; ++i) {
    auto& internalID =
        dataVector->getValue<common::internalID_t>(startOffset + i);
    if (internalIDSet.contains(internalID)) {
      return false;
    }
    internalIDSet.insert(internalID);
  }
  return true;
}

// Note: this executor is only used for isTrail and isAcyclic. So we add some
// ad-hoc optimization into executor, e.g. internalIDSet. A more general
// implementation can be done once needed. But pay attention to the performance
// drop. Depends on how bad it becomes, we may want to implement customized
// executors.
struct UnaryPathExecutor {
  static void executeNodeIDs(common::ValueVector& input,
                             common::SelectionVector& inputSelVector,
                             common::ValueVector& result) {
    auto nodesFieldIdx = 0;
    KU_ASSERT(nodesFieldIdx ==
              common::StructType::getFieldIdx(input.dataType,
                                              common::InternalKeyword::NODES));
    auto nodesVector =
        common::StructVector::getFieldVector(&input, nodesFieldIdx).get();
    auto internalIDFieldIdx = 0;
    execute(inputSelVector, *nodesVector, internalIDFieldIdx, result);
  }

  static void executeRelIDs(common::ValueVector& input,
                            common::SelectionVector& inputSelVector,
                            common::ValueVector& result) {
    auto relsFieldIdx = 1;
    KU_ASSERT(relsFieldIdx ==
              common::StructType::getFieldIdx(input.dataType,
                                              common::InternalKeyword::RELS));
    auto relsVector =
        common::StructVector::getFieldVector(&input, relsFieldIdx).get();
    auto internalIDFieldIdx = 3;
    execute(inputSelVector, *relsVector, internalIDFieldIdx, result);
  }

  static bool selectNodeIDs(common::ValueVector& input,
                            common::SelectionVector& selectionVector) {
    auto nodesFieldIdx = 0;
    KU_ASSERT(nodesFieldIdx ==
              common::StructType::getFieldIdx(input.dataType,
                                              common::InternalKeyword::NODES));
    auto nodesVector =
        common::StructVector::getFieldVector(&input, nodesFieldIdx).get();
    auto internalIDFieldIdx = 0;
    return select(input.state->getSelVector(), *nodesVector, internalIDFieldIdx,
                  selectionVector);
  }

  static bool selectRelIDs(common::ValueVector& input,
                           common::SelectionVector& selectionVector) {
    auto relsFieldIdx = 1;
    KU_ASSERT(relsFieldIdx ==
              common::StructType::getFieldIdx(input.dataType,
                                              common::InternalKeyword::RELS));
    auto relsVector =
        common::StructVector::getFieldVector(&input, relsFieldIdx).get();
    auto internalIDFieldIdx = 3;
    return select(input.state->getSelVector(), *relsVector, internalIDFieldIdx,
                  selectionVector);
  }

 private:
  static void execute(const common::SelectionVector& inputSelVector,
                      common::ValueVector& listVector,
                      common::struct_field_idx_t fieldIdx,
                      common::ValueVector& result) {
    auto listDataVector = common::ListVector::getDataVector(&listVector);
    KU_ASSERT(fieldIdx ==
              common::StructType::getFieldIdx(listDataVector->dataType,
                                              common::InternalKeyword::ID));
    auto internalIDsVector =
        common::StructVector::getFieldVector(listDataVector, fieldIdx).get();
    std::unordered_set<common::nodeID_t, InternalIDHasher> internalIDSet;
    if (inputSelVector.isUnfiltered()) {
      for (auto i = 0u; i < inputSelVector.getSelSize(); ++i) {
        auto& listEntry = listVector.getValue<common::list_entry_t>(i);
        bool isTrail = isAllInternalIDDistinct(
            internalIDsVector, listEntry.offset, listEntry.size, internalIDSet);
        result.setValue<bool>(i, isTrail);
      }
    } else {
      for (auto i = 0u; i < inputSelVector.getSelSize(); ++i) {
        auto pos = inputSelVector[i];
        auto& listEntry = listVector.getValue<common::list_entry_t>(pos);
        bool isTrail = isAllInternalIDDistinct(
            internalIDsVector, listEntry.offset, listEntry.size, internalIDSet);
        result.setValue<bool>(pos, isTrail);
      }
    }
  }

  static bool select(const common::SelectionVector& inputSelVector,
                     common::ValueVector& listVector,
                     common::struct_field_idx_t fieldIdx,
                     common::SelectionVector& selectionVector) {
    auto listDataVector = common::ListVector::getDataVector(&listVector);
    KU_ASSERT(fieldIdx ==
              common::StructType::getFieldIdx(listDataVector->dataType,
                                              common::InternalKeyword::ID));
    auto internalIDsVector =
        common::StructVector::getFieldVector(listDataVector, fieldIdx).get();
    std::unordered_set<common::nodeID_t, InternalIDHasher> internalIDSet;
    auto numSelectedValues = 0u;
    auto buffer = selectionVector.getMutableBuffer();
    if (inputSelVector.isUnfiltered()) {
      for (auto i = 0u; i < inputSelVector.getSelSize(); ++i) {
        auto& listEntry = listVector.getValue<common::list_entry_t>(i);
        bool isTrail = isAllInternalIDDistinct(
            internalIDsVector, listEntry.offset, listEntry.size, internalIDSet);
        buffer[numSelectedValues] = i;
        numSelectedValues += isTrail;
      }
    } else {
      for (auto i = 0u; i < inputSelVector.getSelSize(); ++i) {
        auto pos = inputSelVector[i];
        auto& listEntry = listVector.getValue<common::list_entry_t>(pos);
        bool isTrail = isAllInternalIDDistinct(
            internalIDsVector, listEntry.offset, listEntry.size, internalIDSet);
        buffer[numSelectedValues] = pos;
        numSelectedValues += isTrail;
      }
    }
    selectionVector.setSelSize(numSelectedValues);
    return numSelectedValues > 0;
  }
};

}  // namespace function
}  // namespace gs
