#pragma once
#include <cmath>
#include <algorithm>
#include <functional>
#include "Base/Base.h"

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