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

#include <string>

namespace neug {

/// Thread-safe RFC4122 v4 UUID generator.  Internally uses a thread_local
/// mt19937 seeded once per thread from std::random_device, so generate()
/// has no per-call seeding cost and is safe to call concurrently from
/// multiple threads.
class UUIDGenerator {
 public:
  static std::string Generate();
};

}  // namespace neug
