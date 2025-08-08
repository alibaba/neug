#pragma once

#include <chrono>
#include <string>

#include "neug/compiler/common/assert.h"
#include "neug/utils/exception/exception.h"

namespace gs {
namespace common {

class Timer {
 public:
  void start() {
    finished = false;
    startTime = std::chrono::high_resolution_clock::now();
  }

  void stop() {
    stopTime = std::chrono::high_resolution_clock::now();
    finished = true;
  }

  double getDuration() const {
    if (finished) {
      auto duration = stopTime - startTime;
      return (double) std::chrono::duration_cast<std::chrono::microseconds>(
                 duration)
          .count();
    }
    THROW_EXCEPTION_WITH_FILE_LINE("Timer is still running.");
  }

  uint64_t getElapsedTimeInMS() const {
    auto now = std::chrono::high_resolution_clock::now();
    auto duration = now - startTime;
    auto count =
        std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
    KU_ASSERT(count >= 0);
    return count;
  }

 private:
  std::chrono::time_point<std::chrono::high_resolution_clock> startTime;
  std::chrono::time_point<std::chrono::high_resolution_clock> stopTime;
  bool finished = false;
};

}  // namespace common
}  // namespace gs
