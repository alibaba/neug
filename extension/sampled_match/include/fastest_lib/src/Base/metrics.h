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
#include <cmath>
#include <algorithm>
#include <functional>
#include "Base/base.h"

double identity(double x) {return x;}
double square(double x) {return x*x;}

double QError(double y_true, double y_measured) {
    return std::max(y_measured, 1.0) / std::max(y_true, 1.0);
}

double logQError(double y_true, double y_measured) {
    return log10(QError(y_true, y_measured));
}

double Total(const std::vector<dict>& results, const std::string& which, const std::function<double(double)>& func = identity) {
    double total = 0.0;
    for (auto it : results) {
        total += func(std::any_cast<double>(it[which]));
    }
    return total;
}

double Average(const std::vector<dict>& results, const std::string& which, const std::function<double(double)>& func = identity) {
    double total = Total(results, which, func);
    return total / results.size();
}

double Std(const std::vector<dict>& results, const std::string& which) {
    double total = Total(results, which, identity);
    double sqtotal = Total(results, which, square);
    return sqrt((sqtotal - total * total) / results.size());
}