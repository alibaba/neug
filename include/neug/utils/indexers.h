/** Copyright 2020 Alibaba Group Holding Limited.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
#pragma once

#include "neug/utils/id_indexer.h"

#include "neug/utils/property/types.h"

namespace neug {

using IndexerType = LFIndexer<vid_t>;

template <typename KEY_T>
using IndexerBuilderType = IdIndexer<KEY_T, vid_t>;

}  // namespace neug
