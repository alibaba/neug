#ifndef GLOBAL_TIMER_H_
#define GLOBAL_TIMER_H_

#include <chrono>
#include <ctime>
class Timer {
public:
    Timer() : time(0.0) {}
    ~Timer() {}

    void Start() { s = std::chrono::high_resolution_clock::now(); }

    void Stop() {
        e = std::chrono::high_resolution_clock::now();
        time += std::chrono::duration<double, std::milli>(e - s).count();
        s = std::chrono::high_resolution_clock::now();
    }

    void Add(const Timer &other) { time += other.time; }

    double Peek() { Stop(); return std::chrono::duration<double, std::milli>(e - s).count(); }
    double GetTime() { return time; }
    double time;

private:
    std::chrono::high_resolution_clock::time_point s, e;
};
#endif
