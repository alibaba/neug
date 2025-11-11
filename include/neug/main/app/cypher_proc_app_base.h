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

#ifndef INCLUDE_NEUG_MAIN_APP_CYPHER_PROC_APP_BASE_H_
#define INCLUDE_NEUG_MAIN_APP_CYPHER_PROC_APP_BASE_H_

#include <string>
#include <tuple>

// Disable class-memaccess warning to facilitate compilation with gcc>7
// https://github.com/Tencent/rapidjson/issues/1700
#pragma GCC diagnostic push
#if defined(__GNUC__) && __GNUC__ >= 8
#pragma GCC diagnostic ignored "-Wclass-memaccess"
#endif
#include <rapidjson/document.h>
#pragma GCC diagnostic pop

#include "neug/generated/proto/plan/results.pb.h"
#include "neug/generated/proto/plan/stored_procedure.pb.h"
#include "neug/main/app/app_base.h"
#include "neug/main/neug_db_session.h"
#include "neug/utils/property/types.h"
#include "neug/utils/service_utils.h"

namespace gs {

template <size_t I, typename TUPLE_T, typename... ARGS>
inline bool parse_input_argument_from_proto_impl(
    TUPLE_T& tuple,
    const google::protobuf::RepeatedPtrField<procedure::Argument>& args) {
  if constexpr (I == sizeof...(ARGS)) {
    return true;
  } else {
    auto& type = std::get<I>(tuple);
    auto& argument = args.Get(I);
    if (argument.value_case() != procedure::Argument::kConst) {
      LOG(ERROR) << "Expect a const value for input param, but got "
                 << argument.value_case();
      return false;
    }
    auto& value = argument.const_();
    auto item_case = value.item_case();
    if (item_case == ::common::Value::kI32) {
      if constexpr (std::is_same<int32_t,
                                 std::tuple_element_t<I, TUPLE_T>>::value) {
        type = value.i32();
      } else {
        LOG(ERROR) << "Type mismatch: " << item_case << "at " << I;
        return false;
      }
    } else if (item_case == ::common::Value::kI64) {
      if constexpr (std::is_same<int64_t,
                                 std::tuple_element_t<I, TUPLE_T>>::value) {
        type = value.i64();
      } else {
        LOG(ERROR) << "Type mismatch: " << item_case << "at " << I;
        return false;
      }
    } else if (item_case == ::common::Value::kF64) {
      if constexpr (std::is_same<double,
                                 std::tuple_element_t<I, TUPLE_T>>::value) {
        type = value.f64();
      } else {
        LOG(ERROR) << "Type mismatch: " << item_case << "at " << I;
        return false;
      }
    } else if (item_case == ::common::Value::kStr) {
      if constexpr (std::is_same<std::string,
                                 std::tuple_element_t<I, TUPLE_T>>::value) {
        type = value.str();
      } else {
        LOG(ERROR) << "Type mismatch: " << item_case << "at " << I;
        return false;
      }
    } else {
      LOG(ERROR) << "Not recognizable param type" << item_case;
      return false;
    }
    return parse_input_argument_from_proto_impl<I + 1, TUPLE_T, ARGS...>(tuple,
                                                                         args);
  }
}

template <typename... ARGS>
inline bool parse_input_argument_from_proto(std::tuple<ARGS...>& tuple,
                                            std::string_view sv) {
  if (sv.size() == 0) {
    VLOG(10) << "No arguments found in input";
    return true;
  }
  procedure::Query cur_query;
  if (!cur_query.ParseFromArray(sv.data(), sv.size())) {
    LOG(ERROR) << "Fail to parse query from input content";
    return false;
  }
  auto& args = cur_query.arguments();
  if (args.size() != sizeof...(ARGS)) {
    LOG(ERROR) << "Arguments size mismatch: " << args.size() << " vs "
               << sizeof...(ARGS);
    return false;
  }
  return parse_input_argument_from_proto_impl<0, std::tuple<ARGS...>, ARGS...>(
      tuple, args);
}

class NeugDBSession;

template <typename... ARGS>
bool deserialize(std::tuple<ARGS...>& tuple, std::string_view sv) {
  if (sv.empty()) {
    return sizeof...(ARGS) == 0;
  }
  auto input_format = static_cast<uint8_t>(sv.back());
  std::string_view payload(sv.data(), sv.size() - 1);

  return parse_input_argument_from_proto(tuple, payload);
}

// for cypher procedure
template <typename... ARGS>
class CypherReadProcAppBase : public ReadAppBase {
 public:
  AppType type() const override { return AppType::kCypherProcedure; }

  virtual results::CollectiveResults Query(const NeugDBSession& db,
                                           ARGS... args) = 0;

  bool Query(const NeugDBSession& db, Decoder& input,
             Encoder& output) override {
    std::string_view sv(input.data(), input.size());

    std::tuple<ARGS...> tuple;
    if (!deserialize<ARGS...>(tuple, sv)) {
      LOG(ERROR) << "Failed to deserialize arguments";
      return false;
    }

    // unpack tuple
    auto res = unpackedAndInvoke(db, tuple);
    // write output
    std::string out;
    res.SerializeToString(&out);

    output.put_string(out);
    return true;
  }

  results::CollectiveResults unpackedAndInvoke(const NeugDBSession& db,
                                               std::tuple<ARGS...>& tuple) {
    return std::apply(
        [this, &db](ARGS... args) { return this->Query(db, args...); }, tuple);
  }
};

template <typename... ARGS>
class CypherWriteProcAppBase : public WriteAppBase {
 public:
  AppType type() const override { return AppType::kCypherProcedure; }

  virtual results::CollectiveResults Query(NeugDBSession& db, ARGS... args) = 0;

  bool Query(NeugDBSession& db, Decoder& input, Encoder& output) override {
    std::string_view sv(input.data(), input.size());

    std::tuple<ARGS...> tuple;
    if (!deserialize<ARGS...>(tuple, sv)) {
      LOG(ERROR) << "Failed to deserialize arguments";
      return false;
    }

    // unpack tuple
    auto res = unpackedAndInvoke(db, tuple);
    // write output
    std::string out;
    res.SerializeToString(&out);

    output.put_string(out);
    return true;
  }

  results::CollectiveResults unpackedAndInvoke(NeugDBSession& db,
                                               std::tuple<ARGS...>& tuple) {
    return std::apply([this, &db](ARGS... args) { this->Query(db, args...); },
                      tuple);
  }
};

}  // namespace gs

#endif  // INCLUDE_NEUG_MAIN_APP_CYPHER_PROC_APP_BASE_H_
