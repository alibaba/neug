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

#include "neug/generated/proto/response/response.pb.h"
#include "neug/main/query_result.h"
namespace neug {

class Encoder;
class StorageReadInterface;

namespace execution {

class Context;
class Sink {
 public:
  static void sink_results(const Context& ctx,
                           const StorageReadInterface& graph,
                           neug::QueryResponse* response);
};

}  // namespace execution
}  // namespace neug
