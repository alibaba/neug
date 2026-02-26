/** Copyright 2020 Alibaba Group Holding Limited.
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

#include "neug/execution/expression/exprs/extract_expr.h"
#include "neug/generated/proto/plan/expr.pb.h"

namespace neug {
namespace execution {

class BindedExtractExpr : public VertexExprBase,
                          public EdgeExprBase,
                          public RecordExprBase {
 public:
  BindedExtractExpr(std::unique_ptr<BindedExprBase>&& expr,
                    const ::common::Extract& extract_type)
      : expr_(std::move(expr)),
        extract_type_(extract_type),
        type_(DataType::INT64) {}

  const DataType& type() const override { return type_; }

  Value eval_record(const Context& ctx, size_t idx) const override {
    const auto& val = expr_->Cast<RecordExprBase>().eval_record(ctx, idx);
    return eval_impl(extract_type_, val);
  }

  Value eval_vertex(label_t v_label, vid_t v_id) const override {
    const auto& val = expr_->Cast<VertexExprBase>().eval_vertex(v_label, v_id);
    return eval_impl(extract_type_, val);
  }
  Value eval_edge(const LabelTriplet& label, vid_t src, vid_t dst,
                  const void* data_ptr) const override {
    const auto& val =
        expr_->Cast<EdgeExprBase>().eval_edge(label, src, dst, data_ptr);
    return eval_impl(extract_type_, val);
  }

 private:
  static Value eval_impl(const ::common::Extract& extract_type,
                         const Value& val) {
    if (val.IsNull()) {
      return Value(DataType::INT64);
    }

    if (val.type().id() == DataTypeId::kTimestampMs) {
      int64_t ms = val.GetValue<timestamp_ms_t>().milli_second;
      return Value::INT64(extract_time_from_milli_second(ms, extract_type));
    } else if (val.type().id() == DataTypeId::kDate) {
      switch (extract_type.interval()) {
      case ::common::Extract::YEAR:
        return Value::INT64(val.GetValue<date_t>().year());
      case ::common::Extract::MONTH:
        return Value::INT64(val.GetValue<date_t>().month());
      case ::common::Extract::DAY:
        return Value::INT64(val.GetValue<date_t>().day());
      default:
        LOG(FATAL) << "not support: " << extract_type.DebugString();
        return Value(DataType::INT64);
      }
    } else if (val.type().id() == DataTypeId::kInterval) {
      auto interval = val.GetValue<interval_t>();
      switch (extract_type.interval()) {
      case ::common::Extract::YEAR:
        return Value::INT64(interval.year());
      case ::common::Extract::MONTH:
        return Value::INT64(interval.month());
      case ::common::Extract::DAY:
        return Value::INT64(interval.day());
      case ::common::Extract::HOUR:
        return Value::INT64((interval.hour()));
      case ::common::Extract::MINUTE:
        return Value::INT64((interval.minute()));
      case ::common::Extract::SECOND:
        return Value::INT64((interval.second()));
      case ::common::Extract::MILLISECOND:
        return Value::INT64(interval.millisecond());
      default:
        LOG(FATAL) << "not support: " << extract_type.DebugString();
        return Value(DataType::INT64);
      }
    } else {
      LOG(FATAL) << "not support: " << val.type().id();
      return Value(DataType::INT64);
    }
  }

  static int64_t extract_time_from_milli_second(int64_t ms,
                                                ::common::Extract extract) {
    switch (extract.interval()) {
    case ::common::Extract::YEAR:
      return extract_year(ms);
    case ::common::Extract::MONTH:
      return extract_month(ms);
    case ::common::Extract::DAY:
      return extract_day(ms);
    case ::common::Extract::HOUR:
      return extract_hour(ms);
    case ::common::Extract::MINUTE:
      return extract_minute(ms);
    case ::common::Extract::SECOND:
      return extract_second(ms);
    default:
      LOG(FATAL) << "not support";
    }
    return 0;
  }

  static int32_t extract_year(int64_t ms) {
    auto micro_second = ms / 1000;
    struct tm tm;
    gmtime_r(reinterpret_cast<time_t*>(&micro_second), &tm);
    return tm.tm_year + 1900;
  }

  static int32_t extract_month(int64_t ms) {
    auto micro_second = ms / 1000;
    struct tm tm;
    gmtime_r(reinterpret_cast<time_t*>(&micro_second), &tm);
    return tm.tm_mon + 1;
  }

  static int32_t extract_day(int64_t ms) {
    auto micro_second = ms / 1000;
    struct tm tm;
    gmtime_r(reinterpret_cast<time_t*>(&micro_second), &tm);
    return tm.tm_mday;
  }

  static int32_t extract_second(int64_t ms) {
    auto micro_second = ms / 1000;
    struct tm tm;
    gmtime_r(reinterpret_cast<time_t*>(&micro_second), &tm);
    return tm.tm_sec;
  }

  static int32_t extract_hour(int64_t ms) {
    auto micro_second = ms / 1000;
    struct tm tm;
    gmtime_r(reinterpret_cast<time_t*>(&micro_second), &tm);
    return tm.tm_hour;
  }

  static int32_t extract_minute(int64_t ms) {
    auto micro_second = ms / 1000;
    struct tm tm;
    gmtime_r(reinterpret_cast<time_t*>(&micro_second), &tm);
    return tm.tm_min;
  }

  std::unique_ptr<BindedExprBase> expr_;
  ::common::Extract extract_type_;
  DataType type_;
};

std::unique_ptr<BindedExprBase> ExtractExpr::bind(
    const IStorageInterface* storage, const ParamsMap& params) const {
  auto bound_expr = expr_->bind(storage, params);
  return std::make_unique<BindedExtractExpr>(std::move(bound_expr),
                                             extract_type_);
}
}  // namespace execution
}  // namespace neug