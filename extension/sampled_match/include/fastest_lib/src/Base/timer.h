/**
 * Copyright 2020 Alibaba Group Holding Limited.
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
