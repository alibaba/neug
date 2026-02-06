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

namespace results {
class CollectiveResults;
}  // namespace results
namespace neug {

class Encoder;
class StorageReadInterface;

namespace execution {

class Context;
class Sink {
 public:
  static results::CollectiveResults sink(const Context& ctx,
                                         const StorageReadInterface& graph);

  static void sink(const Context& ctx, const StorageReadInterface& graph,
                   Encoder& output);

  static void sink_encoder(const Context& ctx,
                           const StorageReadInterface& graph, Encoder& encoder);
  // for debug
  static void sink_beta(const Context& ctx, const StorageReadInterface& graph,
                        Encoder& output);
};

}  // namespace execution
}  // namespace neug
