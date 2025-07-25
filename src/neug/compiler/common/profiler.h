#pragma once

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "neug/compiler/common/metric.h"

namespace gs {
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
}  // namespace gs
