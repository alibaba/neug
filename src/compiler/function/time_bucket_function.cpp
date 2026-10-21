/**
 * Copyright 2026 Alibaba Group Holding Limited.
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

#include "neug/compiler/function/date/vector_date_functions.h"

#include <algorithm>
#include <chrono>
#include <limits>

#include "date/date.h"
#include "neug/compiler/function/neug_scalar_function.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace function {
namespace {

// Semantics follow DuckDB's
// extension/core_functions/scalar/date/time_bucket.cpp
// (638b09261a334feb2f7deb742fe3996db3c974b1): Monday/month anchors, month-only
// origins, and subtract/bucket/add offsets. Arithmetic here uses NeuG millis.
using common::DataType;
using common::DataTypeId;
constexpr int64_t kDayMs = Interval::MSEC_PER_DAY;
constexpr int64_t kDefaultOriginMs = 10959LL * kDayMs;
constexpr int64_t kDefaultOriginMonths = 360;
enum class BucketWidthKind { MilliSeconds, Month };
enum class Form { Default, Offset, Origin };

int64_t checkedAdd(int64_t lhs, int64_t rhs) {
  if ((rhs > 0 && lhs > std::numeric_limits<int64_t>::max() - rhs) ||
      (rhs < 0 && lhs < std::numeric_limits<int64_t>::min() - rhs)) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "TIME_BUCKET: time calculation out of range");
  }
  return lhs + rhs;
}

// All callers use a positive divisor, so INT64_MIN / -1 cannot occur.
int64_t floorDivide(int64_t value, int64_t divisor) {
  auto result = value / divisor;
  if (value < 0 && value % divisor != 0) {
    --result;
  }
  return result;
}

void checkPrecision(const Interval& interval) {
  if (interval.micros % Interval::MICROS_PER_MSEC != 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "TIME_BUCKET: interval must be whole milliseconds");
  }
}

int64_t intervalMillis(const Interval& interval) {
  checkPrecision(interval);
  // An int32 day count times milliseconds per day fits in int64.
  return checkedAdd(int64_t(interval.days) * kDayMs,
                    interval.micros / Interval::MICROS_PER_MSEC);
}

struct BucketWidth {
  BucketWidthKind kind;
  int64_t value;
};

BucketWidth classifyWidth(const Interval& width) {
  checkPrecision(width);
  if (width.months != 0) {
    if (width.days != 0 || width.micros != 0) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "TIME_BUCKET: month width cannot contain day or time components");
    }
    if (width.months > 0) {
      return {BucketWidthKind::Month, width.months};
    }
  } else {
    auto millis = intervalMillis(width);
    if (millis > 0) {
      return {BucketWidthKind::MilliSeconds, millis};
    }
  }
  THROW_INVALID_ARGUMENT_EXCEPTION(
      "TIME_BUCKET: width must be greater than zero");
}

// Date's conversions pass through system_clock and date::year. Check their
// ranges before invoking them, rather than detecting wrapped dates afterwards.
void checkDayRange(int64_t days) {
  using Duration = std::chrono::system_clock::duration;
  using Days = std::chrono::duration<int64_t, std::ratio<86400>>;
  static const auto minDay =
      date::sys_days{date::year{1} / 1 / 1}.time_since_epoch().count();
  static const auto maxDay =
      date::sys_days{date::year::max() / 12 / 31}.time_since_epoch().count();
  // Convert the clock limits to whole days instead of multiplying days into
  // ticks. Truncation toward zero gives the first/last representable midnight.
  static const auto minClockDay =
      std::chrono::duration_cast<Days>(Duration::min()).count();
  static const auto maxClockDay =
      std::chrono::duration_cast<Days>(Duration::max()).count();
  if (days < minDay || days > maxDay || days < minClockDay ||
      days > maxClockDay) {
    THROW_INVALID_ARGUMENT_EXCEPTION("TIME_BUCKET: date out of range");
  }
  const date::year_month_day ymd{date::sys_days{date::days{days}}};
  if (int(ymd.year()) < 1 || !ymd.ok()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("TIME_BUCKET: date out of range");
  }
}

int32_t dateDays(const Date& value) {
  if (value.year() < 1 || value.year() > int(date::year::max())) {
    THROW_INVALID_ARGUMENT_EXCEPTION("TIME_BUCKET: date out of range");
  }
  const date::year_month_day ymd{date::year{value.year()},
                                 date::month{unsigned(value.month())},
                                 date::day{unsigned(value.day())}};
  if (!ymd.ok()) {
    THROW_INVALID_ARGUMENT_EXCEPTION("TIME_BUCKET: invalid date");
  }
  auto days = date::sys_days{ymd}.time_since_epoch().count();
  checkDayRange(days);
  Date midnight = value;
  midnight.value.internal.hour = 0;
  const auto result = midnight.to_num_days();
  if (result != days) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "TIME_BUCKET: date conversion out of range");
  }
  return result;
}

Date dateFromMillis(int64_t millis) {
  auto days = static_cast<int64_t>(floorDivide(millis, kDayMs));
  checkDayRange(days);
  Date result;
  result.from_num_days(static_cast<int32_t>(days));
  if (dateDays(result) != days) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "TIME_BUCKET: date conversion out of range");
  }
  return result;
}

int64_t epochMonths(int64_t millis) {
  auto d = dateFromMillis(millis);
  return (int64_t(d.year()) - 1970) * 12 + d.month() - 1;
}

int64_t bucketMillis(int64_t width, int64_t ts, int64_t origin) {
  // Compute (ts - origin) mod width without subtracting the timestamps: that
  // difference can overflow even when the resulting bucket is representable.
  auto tsRemainder = ts % width;
  auto originRemainder = origin % width;
  if (tsRemainder < 0) {
    tsRemainder += width;
  }
  if (originRemainder < 0) {
    originRemainder += width;
  }
  auto distance = tsRemainder - originRemainder;
  if (distance < 0) {
    distance += width;
  }
  return checkedAdd(ts, -distance);
}

int64_t bucketMonths(int64_t width, int64_t ts, int64_t originMonths) {
  const auto month = bucketMillis(width, epochMonths(ts), originMonths);
  const auto year = checkedAdd(1970, floorDivide(month, 12));
  if (year < 1 || year > int(date::year::max())) {
    THROW_INVALID_ARGUMENT_EXCEPTION("TIME_BUCKET: date out of range");
  }
  Date result{};
  result.value.integer = 0;
  result.value.internal.year = year;
  result.value.internal.month = month - floorDivide(month, 12) * 12 + 1;
  result.value.internal.day = 1;
  return int64_t(dateDays(result)) * kDayMs;
}

int64_t bucket(const BucketWidth& width, int64_t ts) {
  return width.kind == BucketWidthKind::MilliSeconds
             ? bucketMillis(width.value, ts, kDefaultOriginMs)
             : bucketMonths(width.value, ts, kDefaultOriginMonths);
}

// Reuse Date's calendar arithmetic and DateTime's ordinary +/- path. Only the
// negative partial-day/month path needs its date decomposition corrected.
DateTime checkedShift(DateTime ts, const Interval& offset, bool subtract) {
  checkPrecision(offset);
  Interval delta = offset;
  if (subtract) {
    if (delta.months == std::numeric_limits<int32_t>::min() ||
        delta.days == std::numeric_limits<int32_t>::min() ||
        delta.micros == std::numeric_limits<int64_t>::min()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "TIME_BUCKET: interval inversion out of range");
    }
    delta.invert();
  }
  int64_t monthMillis = 0;
  if (delta.months != 0) {
    const auto oldDate = dateFromMillis(ts.milli_second);
    const auto oldDays = dateDays(oldDate);
    const auto targetMonth =
        int64_t(oldDate.year()) * 12 + oldDate.month() - 1 + delta.months;
    const auto targetYear = floorDivide(targetMonth, 12);
    if (targetYear < 1 || targetYear > int(date::year::max())) {
      THROW_INVALID_ARGUMENT_EXCEPTION("TIME_BUCKET: date out of range");
    }
    // Validate the destination before Date::operator+ converts it through
    // system_clock. Month-end clipping itself stays in Date::operator+.
    const date::year_month_day first{
        date::year{int(targetYear)},
        date::month{unsigned(targetMonth - targetYear * 12 + 1)}, date::day{1}};
    auto firstDay = date::sys_days{first}.time_since_epoch().count();
    const auto last = date::year_month_day_last{
        first.year(), date::month_day_last{first.month()}};
    auto targetDay = int64_t(firstDay) +
                     std::min(unsigned(oldDate.day()), unsigned(last.day())) -
                     1;
    checkDayRange(targetDay);
    Interval months{};
    months.months = delta.months;
    auto newDate = oldDate + months;
    auto newDays = dateDays(newDate);
    if (newDays != targetDay) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "TIME_BUCKET: date conversion out of range");
    }
    // DateTime's ordinary path subtracts two int32 day values.
    auto difference = int64_t(newDays) - oldDays;
    if (difference < std::numeric_limits<int32_t>::min() ||
        difference > std::numeric_limits<int32_t>::max()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "TIME_BUCKET: date difference out of range");
    }
    monthMillis = difference * kDayMs;
  }
  const auto dayMillis = int64_t(delta.days) * kDayMs;
  const auto shift = checkedAdd(checkedAdd(monthMillis, dayMillis),
                                delta.micros / Interval::MICROS_PER_MSEC);
  const auto result = checkedAdd(ts.milli_second, shift);
  if (delta.months != 0 && ts.milli_second < 0 &&
      ts.milli_second % kDayMs != 0) {
    return DateTime(result);
  }
  return subtract ? ts - offset : ts + offset;
}

bool isTemporal(DataTypeId id) {
  return id == DataTypeId::kDate || id == DataTypeId::kTimestampMs;
}

template <DataTypeId T>
int64_t timestampMillis(const Value& value) {
  if constexpr (T == DataTypeId::kDate) {
    return int64_t(dateDays(value.GetValue<Date>())) * kDayMs;
  } else {
    return value.GetValue<DateTime>().milli_second;
  }
}

template <DataTypeId T, Form F>
Value timeBucketExec(const std::vector<Value>& args) {
  constexpr size_t arity = F == Form::Default ? 2 : 3;
  if (args.size() != arity) {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "TIME_BUCKET: invalid number of arguments");
  }
  for (size_t i = 0; i < arity; ++i) {
    auto expected =
        i == 0 || (i == 2 && F == Form::Offset) ? DataTypeId::kInterval : T;
    // Constant NULLs in the IR may lose their type; the registered callback
    // still carries the bound result type. Validate only non-NULL values.
    if (!args[i].IsNull() && args[i].type().id() != expected) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "TIME_BUCKET: invalid bound argument types");
    }
  }
  for (const auto& arg : args) {
    if (arg.IsNull()) {
      return Value(DataType(T));
    }
  }
  const auto width = classifyWidth(args[0].GetValue<Interval>());
  auto ts = timestampMillis<T>(args[1]);
  int64_t result;
  if constexpr (F == Form::Offset) {
    const auto offset = args[2].GetValue<Interval>();
    ts = checkedShift(DateTime(ts), offset, true).milli_second;
    result =
        checkedShift(DateTime(bucket(width, ts)), offset, false).milli_second;
  } else if constexpr (F == Form::Origin) {
    const auto origin = timestampMillis<T>(args[2]);
    result = width.kind == BucketWidthKind::MilliSeconds
                 ? bucketMillis(width.value, ts, origin)
                 : bucketMonths(width.value, ts, epochMonths(origin));
  } else {
    result = bucket(width, ts);
  }
  if constexpr (T == DataTypeId::kDate) {
    return Value::DATE(dateFromMillis(result));
  } else {
    return Value::TIMESTAMPMS(DateTime(result));
  }
}

template <Form F>
std::unique_ptr<FunctionBindData> bindTimeBucket(
    const ScalarBindFuncInput& input) {
  constexpr size_t arity = F == Form::Default ? 2 : 3;
  if (input.arguments.size() != arity) {
    THROW_BINDER_EXCEPTION("TIME_BUCKET: invalid number of arguments");
  }
  auto widthType = input.arguments[0]->getDataType().id();
  auto type = input.arguments[1]->getDataType().id();
  // Width is always INTERVAL, so an untyped NULL or parameter can be inferred.
  if (widthType != DataTypeId::kInterval && widthType != DataTypeId::kUnknown) {
    THROW_BINDER_EXCEPTION("TIME_BUCKET: width must be INTERVAL");
  }
  if constexpr (F != Form::Default) {
    auto third = input.arguments[2]->getDataType().id();
    if (third == DataTypeId::kUnknown) {
      THROW_BINDER_EXCEPTION(
          "TIME_BUCKET: CAST the third argument to INTERVAL, DATE or "
          "TIMESTAMP");
    }
    if constexpr (F == Form::Offset) {
      if (third != DataTypeId::kInterval) {
        THROW_BINDER_EXCEPTION("TIME_BUCKET: offset must be INTERVAL");
      }
    } else {
      if (!isTemporal(third) ||
          (!isTemporal(type) && type != DataTypeId::kUnknown)) {
        THROW_BINDER_EXCEPTION(
            "TIME_BUCKET: ts and origin must be DATE or TIMESTAMP");
      }
      type =
          type == DataTypeId::kTimestampMs || third == DataTypeId::kTimestampMs
              ? DataTypeId::kTimestampMs
              : DataTypeId::kDate;
    }
  }
  if (!isTemporal(type)) {
    THROW_BINDER_EXCEPTION(
        "TIME_BUCKET: ts must have an explicit DATE or TIMESTAMP type");
  }
  std::vector<DataType> parameters{DataType(DataTypeId::kInterval),
                                   DataType(type)};
  if constexpr (F != Form::Default) {
    parameters.emplace_back(F == Form::Offset ? DataTypeId::kInterval : type);
  }
  // An untyped ts can match several overloads. Use the existing homogeneous
  // catalog signature for its inferred type, including the execution callback.
  if (input.arguments[1]->getDataType().id() == DataTypeId::kUnknown) {
    auto* definition = input.definition->ptrCast<NeugScalarFunction>();
    definition->parameterTypes = parameters;
    definition->returnTypeID = type;
    definition->neugExecFunc =
        type == DataTypeId::kDate ? timeBucketExec<DataTypeId::kDate, F>
                                  : timeBucketExec<DataTypeId::kTimestampMs, F>;
    definition->computeSignature();
  }
  return std::make_unique<FunctionBindData>(std::move(parameters),
                                            DataType(type));
}

}  // namespace

function_set TimeBucketFunction::getFunctionSet() {
  function_set functions;
  auto date = std::make_unique<NeugScalarFunction>(
      TimeBucketFuncSet::name,
      std::vector<DataTypeId>{DataTypeId::kInterval, DataTypeId::kDate},
      DataTypeId::kDate, timeBucketExec<DataTypeId::kDate, Form::Default>);
  date->bindFunc = bindTimeBucket<Form::Default>;
  functions.push_back(std::move(date));

  auto timestamp = std::make_unique<NeugScalarFunction>(
      TimeBucketFuncSet::name,
      std::vector<DataTypeId>{DataTypeId::kInterval, DataTypeId::kTimestampMs},
      DataTypeId::kTimestampMs,
      timeBucketExec<DataTypeId::kTimestampMs, Form::Default>);
  timestamp->bindFunc = bindTimeBucket<Form::Default>;
  functions.push_back(std::move(timestamp));
  return functions;
}

function_set TimeBucketOffsetFunction::getFunctionSet() {
  function_set functions;
  auto date = std::make_unique<NeugScalarFunction>(
      TimeBucketFuncSet::name,
      std::vector<DataTypeId>{DataTypeId::kInterval, DataTypeId::kDate,
                              DataTypeId::kInterval},
      DataTypeId::kDate, timeBucketExec<DataTypeId::kDate, Form::Offset>);
  date->bindFunc = bindTimeBucket<Form::Offset>;
  functions.push_back(std::move(date));

  auto timestamp = std::make_unique<NeugScalarFunction>(
      TimeBucketFuncSet::name,
      std::vector<DataTypeId>{DataTypeId::kInterval, DataTypeId::kTimestampMs,
                              DataTypeId::kInterval},
      DataTypeId::kTimestampMs,
      timeBucketExec<DataTypeId::kTimestampMs, Form::Offset>);
  timestamp->bindFunc = bindTimeBucket<Form::Offset>;
  functions.push_back(std::move(timestamp));
  return functions;
}

function_set TimeBucketOriginFunction::getFunctionSet() {
  function_set functions;
  auto dateDate = std::make_unique<NeugScalarFunction>(
      TimeBucketFuncSet::name,
      std::vector<DataTypeId>{DataTypeId::kInterval, DataTypeId::kDate,
                              DataTypeId::kDate},
      DataTypeId::kDate, timeBucketExec<DataTypeId::kDate, Form::Origin>);
  dateDate->bindFunc = bindTimeBucket<Form::Origin>;
  functions.push_back(std::move(dateDate));

  auto dateTimestamp = std::make_unique<NeugScalarFunction>(
      TimeBucketFuncSet::name,
      std::vector<DataTypeId>{DataTypeId::kInterval, DataTypeId::kDate,
                              DataTypeId::kTimestampMs},
      DataTypeId::kTimestampMs,
      timeBucketExec<DataTypeId::kTimestampMs, Form::Origin>);
  dateTimestamp->bindFunc = bindTimeBucket<Form::Origin>;
  functions.push_back(std::move(dateTimestamp));

  auto timestampDate = std::make_unique<NeugScalarFunction>(
      TimeBucketFuncSet::name,
      std::vector<DataTypeId>{DataTypeId::kInterval, DataTypeId::kTimestampMs,
                              DataTypeId::kDate},
      DataTypeId::kTimestampMs,
      timeBucketExec<DataTypeId::kTimestampMs, Form::Origin>);
  timestampDate->bindFunc = bindTimeBucket<Form::Origin>;
  functions.push_back(std::move(timestampDate));

  auto timestampTimestamp = std::make_unique<NeugScalarFunction>(
      TimeBucketFuncSet::name,
      std::vector<DataTypeId>{DataTypeId::kInterval, DataTypeId::kTimestampMs,
                              DataTypeId::kTimestampMs},
      DataTypeId::kTimestampMs,
      timeBucketExec<DataTypeId::kTimestampMs, Form::Origin>);
  timestampTimestamp->bindFunc = bindTimeBucket<Form::Origin>;
  functions.push_back(std::move(timestampTimestamp));
  return functions;
}

function_set TimeBucketFuncSet::getFunctionSet() {
  auto result = TimeBucketFunction::getFunctionSet();
  for (auto& fn : TimeBucketOffsetFunction::getFunctionSet()) {
    result.push_back(std::move(fn));
  }
  for (auto& fn : TimeBucketOriginFunction::getFunctionSet()) {
    result.push_back(std::move(fn));
  }
  return result;
}

}  // namespace function
}  // namespace neug
