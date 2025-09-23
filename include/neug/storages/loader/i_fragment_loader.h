/** Copyright 2020 Alibaba Group Holding Limited.
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

#ifndef STORAGES_RT_MUTABLE_GRAPH_LOADER_FRAGMENT_LOADER_H_
#define STORAGES_RT_MUTABLE_GRAPH_LOADER_FRAGMENT_LOADER_H_

#include "neug/storages/csr/dual_csr.h"
#include "neug/storages/csr/mutable_csr.h"
#include "neug/storages/graph/schema.h"
#include "neug/utils/indexers.h"
#include "neug/utils/property/types.h"

namespace gs {

// For different input format, we should implement different fragment loader.
class IFragmentLoader {
 public:
  virtual ~IFragmentLoader() = default;
  virtual result<bool> LoadFragment() = 0;
};

}  // namespace gs

#endif  // STORAGES_RT_MUTABLE_GRAPH_LOADER_FRAGMENT_LOADER_H_