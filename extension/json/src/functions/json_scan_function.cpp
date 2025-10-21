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

#include <arrow/array.h>
#include <arrow/builder.h>
#include <arrow/type.h>
#include <glog/logging.h>
#include <rapidjson/document.h>
#include <rapidjson/filereadstream.h>
#include <rapidjson/istreamwrapper.h>
#include <algorithm>
#include <fstream>

#include "json_scan_function.h"
#include "neug/execution/common/columns/arrow_context_column.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/pb_utils.h"
#include "neug/utils/proto/plan/physical.pb.h"

using namespace gs::common;
using namespace gs::function;
using namespace gs::runtime;
namespace gs {
namespace extension {

std::unique_ptr<JsonScanFuncInput> JsonScanFunction::bindFunc(
    const gs::Schema& schema, const gs::runtime::ContextMeta& ctx_meta,
    const ::physical::PhysicalPlan& plan, int op_idx) {
  LOG(INFO) << "[JsonScan] Binding function from PhysicalPlan";

  const auto& physical_opr = plan.query_plan().plan(op_idx);
  if (!physical_opr.opr().has_procedure_call()) {
    THROW_EXTENSION_EXCEPTION("Expected ProcedureCall operator");
  }

  const auto& procedure_call = physical_opr.opr().procedure_call();
  const auto& query = procedure_call.query();

  // Obtain filePath
  std::string filePath;

  if (query.arguments_size() > 0) {
    const auto& first_argument = query.arguments(0);
    if (first_argument.has_const_() && first_argument.const_().item_case() ==
                                           ::common::Value::ItemCase::kStr) {
      filePath = first_argument.const_().str();
    }
  }

  // Obtain columnTypes from MetaData
  std::vector<PropertyType> columnTypes;
  const auto& metadatas = physical_opr.meta_data();
  for (const auto& metadata : metadatas) {
    PropertyType type;
    if (!gs::data_type_to_property_type(metadata.type().data_type(), type)) {
      THROW_EXTENSION_EXCEPTION("Unrecognized data type: " +
                                metadata.type().DebugString());
    }
    columnTypes.push_back(type);
  }

  // Validate file exists
  std::ifstream file(filePath);
  if (!file.good()) {
    THROW_EXTENSION_EXCEPTION("JSON file does not exist: " + filePath);
  }
  file.close();

  LOG(INFO) << "[JsonScan] Parsed filePath: " << filePath
            << ", columnTypes: " << columnTypes.size();

  // Create input structure
  return std::make_unique<JsonScanFuncInput>(filePath, columnTypes);
}

Context JsonScanFunction::execFunc(const JsonScanFuncInput& input) {
  LOG(INFO) << "[JsonScan] Executing function for file: " << input.filePath;

  try {
    // Parse JSON file and convert to Arrow arrays
    auto arrowArrays = parseJsonFile(input.filePath, input.columnTypes);

    // Create context and populate with arrow data
    Context ctx;
    if (ctx.col_num() != 0) {
      LOG(ERROR) << "Expect a empty context, but got " << ctx.col_num();
      THROW_EXTENSION_EXCEPTION("Expected empty context");
    }
    auto num_columns = static_cast<int>(arrowArrays.size());
    for (int i = 0; i < num_columns; i++) {
      ArrowArrayContextColumnBuilder column_builder;
      column_builder.push_back(arrowArrays[i]);
      ctx.set(i, column_builder.finish());
    }

    auto num_rows = arrowArrays.empty() ? 0 : arrowArrays[0]->length();
    LOG(INFO) << "[JsonScan] Successfully loaded: got " << num_rows
              << " rows with " << num_columns << " columns";

    return ctx;

  } catch (const std::exception& e) {
    LOG(ERROR) << "JSON parsing failed: " << e.what();
    THROW_EXTENSION_EXCEPTION("JSON parsing failed: " + std::string(e.what()));
  }
}

std::vector<std::shared_ptr<arrow::Array>> JsonScanFunction::parseJsonFile(
    const std::string& filePath, const std::vector<PropertyType>& columnTypes) {
  std::vector<std::vector<std::string>> columnData(columnTypes.size());

  // Parse JSON array format: [{"k1": "v1", "k2": "v2"}, ...]
  std::ifstream ifs(filePath);
  if (!ifs.is_open() || !ifs.good()) {
    THROW_EXTENSION_EXCEPTION("JSON file does not exist: " + filePath);
  }
  rapidjson::IStreamWrapper isw(ifs);

  rapidjson::Document document;
  document.ParseStream(isw);

  if (document.HasParseError()) {
    THROW_EXTENSION_EXCEPTION("JSON parse error at offset " +
                              std::to_string(document.GetErrorOffset()));
  }

  if (!document.IsArray()) {
    THROW_EXTENSION_EXCEPTION("Expected JSON array format");
  }

  if (document.Empty()) {
    std::vector<std::shared_ptr<arrow::Array>> arrowArrays;
    arrowArrays.reserve(columnTypes.size());
    for (size_t i = 0; i < columnTypes.size(); ++i) {
      arrowArrays.push_back(convertJsonValue({}, columnTypes[i]));
    }
    return arrowArrays;
  }

  const auto& first = document[0];
  if (!first.IsObject()) {
    THROW_EXTENSION_EXCEPTION("Expected JSON object in array");
  }

  std::vector<std::string> requiredNames;
  requiredNames.reserve(first.MemberCount());
  for (auto m = first.MemberBegin(); m != first.MemberEnd(); ++m) {
    requiredNames.emplace_back(m->name.GetString());
  }

  if (requiredNames.size() != columnTypes.size()) {
    THROW_EXTENSION_EXCEPTION("JSON object field count (" +
                              std::to_string(requiredNames.size()) +
                              ") does not match expected column count (" +
                              std::to_string(columnTypes.size()) + ")");
  }

  auto valueToString = [](const rapidjson::Value& val) -> std::string {
    if (val.IsString())
      return std::string(val.GetString());
    if (val.IsInt())
      return std::to_string(val.GetInt());
    if (val.IsInt64())
      return std::to_string(val.GetInt64());
    if (val.IsUint())
      return std::to_string(val.GetUint());
    if (val.IsUint64())
      return std::to_string(val.GetUint64());
    if (val.IsDouble())
      return std::to_string(val.GetDouble());
    if (val.IsBool())
      return val.GetBool() ? "true" : "false";
    return "";
  };

  for (auto& obj : document.GetArray()) {
    if (!obj.IsObject()) {
      THROW_EXTENSION_EXCEPTION("Expected JSON object in array");
    }
    for (size_t i = 0; i < requiredNames.size(); ++i) {
      const auto& name = requiredNames[i];
      if (!obj.HasMember(name.c_str())) {
        THROW_EXTENSION_EXCEPTION("Missing required field: " + name);
      }
      const rapidjson::Value& val = obj[name.c_str()];
      columnData[i].push_back(valueToString(val));
    }
  }

  // Convert to Arrow arrays
  std::vector<std::shared_ptr<arrow::Array>> arrowArrays;
  arrowArrays.reserve(columnTypes.size());
  for (size_t i = 0; i < columnTypes.size(); ++i) {
    arrowArrays.push_back(convertJsonValue(columnData[i], columnTypes[i]));
  }

  return arrowArrays;
}

template <typename ArrowBuilderT, typename ConvertFunc>
std::shared_ptr<arrow::Array> buildSimpleArrowArray(
    const std::vector<std::string>& jsonValues, ConvertFunc convertFunc) {
  ArrowBuilderT builder;
  arrow::Status status;

  for (const auto& val : jsonValues) {
    if (val.empty()) {
      status = builder.AppendNull();
    } else {
      try {
        auto convertedVal = convertFunc(val);
        status = builder.Append(convertedVal);
      } catch (...) { status = builder.AppendNull(); }
    }
    if (!status.ok()) {
      THROW_EXTENSION_EXCEPTION("Failed to append value: " + status.ToString());
    }
  }

  std::shared_ptr<arrow::Array> array;
  status = builder.Finish(&array);
  if (!status.ok()) {
    THROW_EXTENSION_EXCEPTION("Failed to finish array: " + status.ToString());
  }
  return array;
}

template <typename ArrowBuilderT, typename ConvertFunc, typename... Args>
std::shared_ptr<arrow::Array> buildComplexArrowArray(
    const std::vector<std::string>& jsonValues, ConvertFunc convertFunc,
    Args&&... args) {
  ArrowBuilderT builder(std::forward<Args>(args)...);
  arrow::Status status;

  for (const auto& val : jsonValues) {
    if (val.empty()) {
      status = builder.AppendNull();
    } else {
      try {
        auto convertedVal = convertFunc(val);
        status = builder.Append(convertedVal);
      } catch (...) { status = builder.AppendNull(); }
    }
    if (!status.ok()) {
      THROW_EXTENSION_EXCEPTION("Failed to append value: " + status.ToString());
    }
  }

  std::shared_ptr<arrow::Array> array;
  status = builder.Finish(&array);
  if (!status.ok()) {
    THROW_EXTENSION_EXCEPTION("Failed to finish array: " + status.ToString());
  }
  return array;
}

std::shared_ptr<arrow::Array> JsonScanFunction::convertJsonValue(
    const std::vector<std::string>& jsonValues, PropertyType targetType) {
  if (targetType == PropertyType::Int32()) {
    return buildSimpleArrowArray<arrow::Int32Builder>(
        jsonValues,
        [](const std::string& val) -> int32_t { return std::stoi(val); });
  } else if (targetType == PropertyType::Int64()) {
    return buildSimpleArrowArray<arrow::Int64Builder>(
        jsonValues,
        [](const std::string& val) -> int64_t { return std::stoll(val); });
  } else if (targetType == PropertyType::Double()) {
    return buildSimpleArrowArray<arrow::DoubleBuilder>(
        jsonValues,
        [](const std::string& val) -> double { return std::stod(val); });
  } else if (targetType == PropertyType::Date()) {
    return buildSimpleArrowArray<arrow::Date32Builder>(
        jsonValues, [](const std::string& val) -> int32_t {
          Date date(val);
          return date.to_num_days();
        });
  } else if (targetType == PropertyType::Bool()) {
    return buildSimpleArrowArray<arrow::BooleanBuilder>(
        jsonValues, [](const std::string& val) -> bool {
          return (val == "true" || val == "1");
        });
  } else if (targetType == PropertyType::DateTime()) {
    return buildComplexArrowArray<arrow::TimestampBuilder>(
        jsonValues,
        [](const std::string& val) -> int64_t {
          DateTime datetime(val);
          return datetime.milli_second;
        },
        arrow::timestamp(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
  } else if (targetType == PropertyType::Interval()) {
    return buildComplexArrowArray<arrow::DurationBuilder>(
        jsonValues,
        [](const std::string& val) -> int64_t {
          Interval interval(val);
          return interval.to_mill_seconds();
        },
        arrow::duration(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
  } else if (targetType == PropertyType::Timestamp()) {
    return buildComplexArrowArray<arrow::TimestampBuilder>(
        jsonValues,
        [](const std::string& val) -> int64_t { return std::stoll(val); },
        arrow::timestamp(arrow::TimeUnit::MILLI), arrow::default_memory_pool());
  } else {  // default PropertyType is String
    return buildSimpleArrowArray<arrow::StringBuilder>(
        jsonValues, [](const std::string& val) -> std::string { return val; });
  }
}

}  // namespace extension
}  // namespace gs