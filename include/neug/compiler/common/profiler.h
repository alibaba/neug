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

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "neug/compiler/common/metric.h"

namespace neug {
namespace common {

class Profiler {
 public:
  TimeMetric* registerTimeMetric(const std::string& key);

  NumericMetric* registerNumericMetric(const std::string& key);

  double sumAllTimeMetricsWithKey(const std::string& key);

  uint64_t sumAllNumericMetricsWithKey(const std::string& key);

 private:
  void addMetric(const std::string& key, std::unique_ptr<Metric> metric);

 public:
  std::mutex mtx;
  bool enabled = false;
  std::unordered_map<std::string, std::vector<std::unique_ptr<Metric>>> metrics;
};

}  // namespace common
}  // namespace neug
