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

/**
 * This file is originally from the FaSTest project
 * (https://github.com/SNUCSE-CTA/FaSTest) Licensed under the MIT License. Modified by
 * Yunkai Lou and Shunyang Li in 2025 to support Neug-specific features.
 */

#pragma once
#include <queue>
#include <cassert>
#include <cstdint>
#include <limits>
#include <vector>
#include <cstring>
#include <random>
#include <fstream>
#include <filesystem>
#include <any>
#include <map>
using std::string;
using std::deque;
using dict = std::map<std::string, std::any>;

inline void AddDict(dict &to, dict &from) {
    for (auto &[key, value] : from) {
        to[key] = value;
    }
}


[[maybe_unused]] static FILE *log_to = stderr;
//FILE *log_to = fopen("/dev/null","w");
[[maybe_unused]] static unsigned long long functionCallCounter = 0;
/**
 * @brief String parsing with specified delimeter
 * @Source Folklore
 */


inline deque<string> parse(string line, const string& del) {
    deque<string> ret;

    size_t pos = 0;
    string token;
    while((pos = line.find(del)) != string::npos) {
        token = line.substr(0, pos);
        ret.push_back(token);
        line.erase(0, pos + del.length());
    }
    ret.push_back(line);
    return ret;
}

[[maybe_unused]] static std::streampos fileSize(const char *filePath) {

    std::streampos fsize = 0;
    std::ifstream file(filePath, std::ios::binary);

    fsize = file.tellg();
    file.seekg(0, std::ios::end);
    fsize = file.tellg() - fsize;
    file.close();

    return fsize;
}

inline bool CreateDirectory(const std::string & dirName) {
    std::error_code err;
    if (!std::filesystem::create_directories(dirName, err)) {
        if (std::filesystem::exists(dirName)) {
            return true;
        }
        printf("CREATEDIR: FAILED to create [%s], err:%s\n", dirName.c_str(), err.message().c_str());
        return false;
    }
    return true;
}

namespace std {
    // from boost (functional/hash):
    // see http://www.boost.org/doc/libs/1_35_0/doc/html/hash/combine.html template
    template <class T> inline void combine(std::size_t &seed, T const &v) {
        seed ^= hash<T>()(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
    }

    template<>
    struct hash<std::pair<int, int>> {
        auto operator()(const std::pair<int, int> &x) const -> size_t {
            std::size_t seed = 17;
            combine(seed, x.first);
            combine(seed, x.second);
            return seed;
        }
    };
}  // namespace std

template <typename T>
void EraseIndex(std::vector<T> &vec, int& idx) {
    vec[idx] = vec.back();
    vec.pop_back();
    --idx;
}

