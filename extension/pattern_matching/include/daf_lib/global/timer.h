#ifndef GLOBAL_TIMER_H_
#define GLOBAL_TIMER_H_

#include <chrono>
#include <ctime>

namespace daf {
class Timer {
 public:
  Timer() : time(0.0) {}
  ~Timer() {}

  void Start() { s = std::chrono::high_resolution_clock::now(); }

  void Stop() {
    e = std::chrono::high_resolution_clock::now();
    time += std::chrono::duration<double, std::milli>(e - s).count();
  }

  void Add(const Timer &other) { time += other.time; }

  double GetTime() { return time; }

 private:
  std::chrono::high_resolution_clock::time_point s, e;
  double time;
};
}  // namespace daf

#endif  // GLOBAL_TIMER_H_
