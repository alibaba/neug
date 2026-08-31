/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <functional>
#include <vector>

#include "neug/utils/property/types.h"
#include "neug/utils/result.h"

namespace neug {

struct IndexMeta;
class PropertyGraph;
class StorageIndex;

bool IsHNSWIndex(const IndexMeta& meta);
bool IsCosineMetric(const IndexMeta& meta);
bool ParseCosineNormalizeOption(IndexMeta& meta);
bool UsesCosineNormalization(const IndexMeta& meta);

// Adds index entries for newly inserted vertices. COW callers use
// prepare_index to detach each index before its first mutation.
Status AddBatchVertexIndexData(
    PropertyGraph& graph, label_t label, const std::vector<vid_t>& vids,
    const std::function<Status(StorageIndex&)>& prepare_index = nullptr);

}  // namespace neug
