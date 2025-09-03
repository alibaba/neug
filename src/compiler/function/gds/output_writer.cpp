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

#include "neug/compiler/function/gds/gds_frontier.h"
#include "neug/compiler/function/gds/rj_output_writer.h"
#include "neug/compiler/main/client_context.h"
#include "neug/utils/exception/exception.h"

using namespace gs::common;
using namespace gs::processor;

namespace gs {
namespace function {

RJOutputWriter::RJOutputWriter(main::ClientContext* context,
                               NodeOffsetMaskMap* outputNodeMask,
                               nodeID_t sourceNodeID)
    : context{context},
      outputNodeMask{outputNodeMask},
      sourceNodeID_{sourceNodeID} {
  srcNodeIDVector = createVector(LogicalType::INTERNAL_ID());
  dstNodeIDVector = createVector(LogicalType::INTERNAL_ID());
  srcNodeIDVector->setValue<nodeID_t>(0, sourceNodeID);
}

void RJOutputWriter::pinOutputNodeMask(table_id_t tableID) {
  if (outputNodeMask != nullptr) {
    outputNodeMask->pin(tableID);
  }
}

bool RJOutputWriter::inOutputNodeMask(common::offset_t offset) {
  if (outputNodeMask == nullptr) {
    return true;
  }
  auto mask = outputNodeMask->getPinnedMask();
  if (!mask->isEnabled()) {
    return true;
  }
  return mask->isMasked(offset);
}

std::unique_ptr<ValueVector> RJOutputWriter::createVector(
    const LogicalType& type) {
  auto vector =
      std::make_unique<ValueVector>(type.copy(), context->getMemoryManager());
  vector->state = DataChunkState::getSingleValueDataChunkState();
  vectors.push_back(vector.get());
  return vector;
}

PathsOutputWriter::PathsOutputWriter(main::ClientContext* context,
                                     NodeOffsetMaskMap* outputNodeMask,
                                     nodeID_t sourceNodeID,
                                     PathsOutputWriterInfo info,
                                     BaseBFSGraph& bfsGraph)
    : RJOutputWriter{context, outputNodeMask, sourceNodeID},
      info{info},
      bfsGraph{bfsGraph} {
  lengthVector = createVector(LogicalType::UINT16());
  if (info.writeEdgeDirection) {
    directionVector = createVector(LogicalType::LIST(LogicalType::BOOL()));
  }
  if (info.writePath) {
    pathNodeIDsVector =
        createVector(LogicalType::LIST(LogicalType::INTERNAL_ID()));
    pathEdgeIDsVector =
        createVector(LogicalType::LIST(LogicalType::INTERNAL_ID()));
  }
}

static void addListEntry(ValueVector* vector, uint64_t length) {
  vector->resetAuxiliaryBuffer();
  auto entry = ListVector::addList(vector, length);
  KU_ASSERT(entry.offset == 0);
  vector->setValue(0, entry);
}

static ParentList* getTop(const std::vector<ParentList*>& path) {
  return path[path.size() - 1];
}

bool PathsOutputWriter::updateCounterAndTerminate(LimitCounter* counter) {
  if (counter != nullptr) {
    counter->increase(1);
    return counter->exceedLimit();
  }
  return false;
}

ParentList* PathsOutputWriter::findFirstParent(offset_t dstOffset) const {
  auto result = bfsGraph.getParentListHead(dstOffset);
  if (!info.hasNodeMask() && info.semantic == PathSemantic::WALK) {
    return result;
  }
  while (result) {
    if (checkPathNodeMask(result) && result->getIter() >= info.lowerBound) {
      break;
    }
    result = result->getNextPtr();
  }
  return result;
}

bool PathsOutputWriter::isNextViable(
    ParentList* next, const std::vector<ParentList*>& path) const {
  if (next == nullptr) {
    return false;
  }
  auto nextIter = next->getIter();
  if (path.size() == 1) {
    return nextIter >= info.lowerBound;
  }
  if (nextIter == getTop(path)->getIter()) {
    return true;
  }
  return false;
}

bool PathsOutputWriter::checkPathNodeMask(ParentList* element) const {
  if (!info.hasNodeMask() || element->getIter() == 1) {
    return true;
  }
  return info.pathNodeMask->valid(element->getNodeID());
}

bool PathsOutputWriter::checkAppendSemantic(
    const std::vector<ParentList*>& path, ParentList* candidate) const {
  switch (info.semantic) {
  case PathSemantic::WALK:
    return true;
  case PathSemantic::TRAIL:
    return isAppendTrail(path, candidate);
  case PathSemantic::ACYCLIC:
    return isAppendAcyclic(path, candidate);
  default:
    KU_UNREACHABLE;
  }
}

bool PathsOutputWriter::checkReplaceTopSemantic(
    const std::vector<ParentList*>& path, ParentList* candidate) const {
  switch (info.semantic) {
  case PathSemantic::WALK:
    return true;
  case PathSemantic::TRAIL:
    return isReplaceTopTrail(path, candidate);
  case PathSemantic::ACYCLIC:
    return isReplaceTopAcyclic(path, candidate);
  default:
    KU_UNREACHABLE;
  }
}

bool PathsOutputWriter::isAppendTrail(const std::vector<ParentList*>& path,
                                      ParentList* candidate) const {
  for (auto& element : path) {
    if (candidate->getEdgeID() == element->getEdgeID()) {
      return false;
    }
  }
  return true;
}

bool PathsOutputWriter::isAppendAcyclic(const std::vector<ParentList*>& path,
                                        ParentList* candidate) const {
  for (auto i = 1u; i < path.size() - 1; ++i) {
    if (candidate->getNodeID() == path[i]->getNodeID()) {
      return false;
    }
  }
  return true;
}

bool PathsOutputWriter::isReplaceTopTrail(const std::vector<ParentList*>& path,
                                          ParentList* candidate) const {
  for (auto i = 0u; i < path.size() - 1; ++i) {
    if (candidate->getEdgeID() == path[i]->getEdgeID()) {
      return false;
    }
  }
  return true;
}

bool PathsOutputWriter::isReplaceTopAcyclic(
    const std::vector<ParentList*>& path, ParentList* candidate) const {
  for (auto i = 1u; i < path.size() - 1; ++i) {
    if (candidate->getNodeID() == path[i]->getNodeID()) {
      return false;
    }
  }
  return true;
}

static void setLength(ValueVector* vector, uint16_t length) {
  KU_ASSERT(vector->dataType.getLogicalTypeID() == LogicalTypeID::UINT16);
  vector->setValue<uint16_t>(0, length);
}

void PathsOutputWriter::beginWritePath(idx_t length) const {
  KU_ASSERT(info.writePath);
  addListEntry(pathNodeIDsVector.get(), length > 1 ? length - 1 : 0);
  addListEntry(pathEdgeIDsVector.get(), length);
  if (info.writeEdgeDirection) {
    addListEntry(directionVector.get(), length);
  }
}

void PathsOutputWriter::writePath(const std::vector<ParentList*>& path) const {
  setLength(lengthVector.get(), path.size());
  if (!info.writePath) {
    return;
  }
  beginWritePath(path.size());
  if (path.size() == 0) {
    return;
  }
  if (!info.flipPath) {
    writePathBwd(path);
  } else {
    writePathFwd(path);
  }
}

void PathsOutputWriter::writePathFwd(
    const std::vector<ParentList*>& path) const {
  auto length = path.size();
  for (auto i = 0u; i < length - 1; i++) {
    auto p = path[i];
    addNode(p->getNodeID(), i);
    addEdge(p->getEdgeID(), p->isFwdEdge(), i);
  }
  auto lastPathElement = path[length - 1];
  addEdge(lastPathElement->getEdgeID(), lastPathElement->isFwdEdge(),
          length - 1);
}

void PathsOutputWriter::writePathBwd(
    const std::vector<ParentList*>& path) const {
  auto length = path.size();
  for (auto i = 1u; i < length; i++) {
    auto p = path[length - 1 - i];
    addNode(p->getNodeID(), i - 1);
    addEdge(p->getEdgeID(), p->isFwdEdge(), i);
  }
  auto lastPathElement = path[length - 1];
  addEdge(lastPathElement->getEdgeID(), lastPathElement->isFwdEdge(), 0);
}

void PathsOutputWriter::addEdge(relID_t edgeID, bool fwdEdge, sel_t pos) const {
  ListVector::getDataVector(pathEdgeIDsVector.get())->setValue(pos, edgeID);
  if (info.writeEdgeDirection) {
    ListVector::getDataVector(directionVector.get())->setValue(pos, fwdEdge);
  }
}

void PathsOutputWriter::addNode(nodeID_t nodeID, sel_t pos) const {
  ListVector::getDataVector(pathNodeIDsVector.get())->setValue(pos, nodeID);
}

}  // namespace function
}  // namespace gs
