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

#include <random>
#include <string>

namespace neug {

class UUIDGenerator {
 public:
  UUIDGenerator() : rd_(), gen_(rd_()), dis_(0, 15) {}
  ~UUIDGenerator() = default;

  std::string generate();

 private:
  std::random_device rd_;
  std::mt19937 gen_;
  std::uniform_int_distribution<> dis_;
};

}  // namespace neug