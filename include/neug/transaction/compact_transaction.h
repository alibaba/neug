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
#pragma once

#include "neug/utils/property/types.h"
#include "neug/utils/serialization/in_archive.h"

namespace neug {

class PropertyGraph;
class IWalWriter;
class IVersionManager;

class CompactTransaction {
 public:
  CompactTransaction(PropertyGraph& graph, IWalWriter& logger,
                     IVersionManager& vm, bool compact_csr, float reserve_ratio,
                     timestamp_t timestamp);
  ~CompactTransaction();

  timestamp_t timestamp() const;

  bool Commit();

  void Abort();

 private:
  PropertyGraph& graph_;
  IWalWriter& logger_;
  IVersionManager& vm_;
  bool compact_csr_;
  float reserve_ratio_;
  timestamp_t timestamp_;

  InArchive arc_;
};

}  // namespace neug
