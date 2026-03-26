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
#ifndef NEUG_TESTS_UNITTTEST_STORAGES_UTILS_H_
#define NEUG_TESTS_UNITTTEST_STORAGES_UTILS_H_

#include <random>
#include <vector>

#include "neug/main/connection.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/arrow_utils.h"
#include "neug/utils/property/types.h"

class GeneratedRecordBatchSupplier : public neug::IRecordBatchSupplier {
 public:
  GeneratedRecordBatchSupplier(
      std::vector<std::shared_ptr<arrow::RecordBatch>>&& batches)
      : batches_(std::move(batches)) {}
  ~GeneratedRecordBatchSupplier() override = default;

  std::shared_ptr<arrow::RecordBatch> GetNextBatch() override {
    if (batches_.empty()) {
      return nullptr;
    } else {
      auto batch = batches_.back();
      batches_.pop_back();
      return batch;
    }
  }

  int64_t RowNum() const override {
    int64_t total_rows = 0;
    for (const auto& batch : batches_) {
      total_rows += batch->num_rows();
    }
    return total_rows;
  }

 private:
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches_;
};

template <typename EDATA_T>
std::vector<EDATA_T> generate_random_data(size_t len) {
  if constexpr (std::is_integral<EDATA_T>::value) {
    std::vector<EDATA_T> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<EDATA_T> dis(
        std::numeric_limits<EDATA_T>::min(),
        std::numeric_limits<EDATA_T>::max());
    for (size_t i = 0; i < len; ++i) {
      data.push_back(dis(gen));
    }
    return data;
  } else if constexpr (std::is_floating_point<EDATA_T>::value) {
    std::vector<EDATA_T> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<EDATA_T> dis(-1.0, 1.0);
    for (size_t i = 0; i < len; ++i) {
      data.push_back(dis(gen));
    }
    return data;
  } else if constexpr (std::is_same<EDATA_T, neug::EmptyType>::value) {
    return std::vector<EDATA_T>(len, neug::EmptyType());
  } else if constexpr (std::is_same<EDATA_T, neug::Date>::value) {
    std::vector<EDATA_T> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int32_t> dis(0, 365 * 50);  // 50 years
    for (size_t i = 0; i < len; ++i) {
      data.push_back(EDATA_T(dis(gen)));
    }
    return data;
  } else if constexpr (std::is_same<EDATA_T, neug::DateTime>::value) {
    std::vector<EDATA_T> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(
        0, static_cast<int64_t>(365 * 50) * 24 * 3600 *
               1000);  // 50 years in milliseconds
    for (size_t i = 0; i < len; ++i) {
      data.push_back(EDATA_T(dis(gen)));
    }
    return data;
  } else if constexpr (std::is_same<EDATA_T, std::string>::value) {
    std::vector<EDATA_T> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> len_dis(1, 20);
    std::uniform_int_distribution<> char_dis(97, 122);  // a-z
    for (size_t i = 0; i < len; ++i) {
      int str_len = len_dis(gen);
      std::string str;
      for (int j = 0; j < str_len; ++j) {
        str += static_cast<char>(char_dis(gen));
      }
      data.push_back(str);
    }
    return data;
  } else if constexpr (std::is_same<EDATA_T, neug::Interval>::value) {
    std::vector<EDATA_T> data;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<int64_t> dis(
        std::numeric_limits<int64_t>::min(),
        std::numeric_limits<int64_t>::max());
    for (size_t i = 0; i < len; ++i) {
      neug::Interval interval;
      interval.from_mill_seconds(dis(gen));
      data.push_back(interval);
    }
    return data;
  } else {
    LOG(FATAL) << "Unsupported data type";
    return {};
  }
}

template <typename EDATA_T>
std::shared_ptr<arrow::Array> convert_to_arrow_array(const EDATA_T* begin,
                                                     const EDATA_T* end) {
  std::shared_ptr<arrow::Array> array = nullptr;
  arrow::Status status;
  if constexpr (std::is_same<EDATA_T, int>::value) {
    arrow::Int32Builder builder;
    for (auto data = begin; data != end; ++data) {
      status = builder.Append(*data);
      if (!status.ok()) {
        LOG(FATAL) << "Failed to append data to arrow array: "
                   << status.ToString();
      }
    }
    status = builder.Finish(&array);
    if (!status.ok()) {
      LOG(FATAL) << "Failed to finish arrow array: " << status.ToString();
    }
  } else if constexpr (std::is_same<EDATA_T, int64_t>::value) {
    arrow::Int64Builder builder;
    for (auto data = begin; data != end; ++data) {
      status = builder.Append(*data);
      if (!status.ok()) {
        LOG(FATAL) << "Failed to append data to arrow array: "
                   << status.ToString();
      }
    }
    status = builder.Finish(&array);
    if (!status.ok()) {
      LOG(FATAL) << "Failed to finish arrow array: " << status.ToString();
    }
  } else if constexpr (std::is_same<EDATA_T, std::string>::value) {
    arrow::StringBuilder builder;
    for (auto data = begin; data != end; ++data) {
      status = builder.Append(*data);
      if (!status.ok()) {
        LOG(FATAL) << "Failed to append data to arrow array: "
                   << status.ToString();
      }
    }
    status = builder.Finish(&array);
    if (!status.ok()) {
      LOG(FATAL) << "Failed to finish arrow array: " << status.ToString();
    }
  } else if constexpr (std::is_same<EDATA_T, float>::value) {
    arrow::FloatBuilder builder;
    for (auto data = begin; data != end; ++data) {
      status = builder.Append(*data);
      if (!status.ok()) {
        LOG(FATAL) << "Failed to append data to arrow array: "
                   << status.ToString();
      }
    }
    status = builder.Finish(&array);
    if (!status.ok()) {
      LOG(FATAL) << "Failed to finish arrow array: " << status.ToString();
    }
  } else if constexpr (std::is_same<EDATA_T, double>::value) {
    arrow::DoubleBuilder builder;
    for (auto data = begin; data != end; ++data) {
      status = builder.Append(*data);
      if (!status.ok()) {
        LOG(FATAL) << "Failed to append data to arrow array: "
                   << status.ToString();
      }
    }
    status = builder.Finish(&array);
    if (!status.ok()) {
      LOG(FATAL) << "Failed to finish arrow array: " << status.ToString();
    }
  } else {
    LOG(FATAL) << "Unsupported data type";
  }
  return array;
}

template <typename T>
std::vector<std::shared_ptr<arrow::Array>> convert_to_arrow_arrays(
    const std::vector<T>& data, int num_chunks) {
  size_t chunk_size = (data.size() + num_chunks - 1) / num_chunks;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (int i = 0; i < num_chunks; ++i) {
    size_t begin = i * chunk_size;
    size_t end = std::min(begin + chunk_size, data.size());
    if (begin >= end) {
      break;
    }
    arrays.push_back(convert_to_arrow_array(&data[begin], &data[end]));
  }
  return arrays;
}

inline std::vector<std::shared_ptr<arrow::RecordBatch>>
convert_to_record_batches(
    const std::vector<std::string>& col_names,
    const std::vector<std::vector<std::shared_ptr<arrow::Array>>>& arrays) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  std::vector<size_t> chunk_sizes;
  for (auto& array : arrays[0]) {
    chunk_sizes.push_back(array->length());
  }
  for (size_t i = 1; i < arrays.size(); ++i) {
    if (arrays[i].size() != chunk_sizes.size()) {
      LOG(FATAL) << "All columns must have the same number of chunks";
    }
    for (size_t j = 0; j < arrays[i].size(); ++j) {
      if (static_cast<size_t>(arrays[i][j]->length()) != chunk_sizes[j]) {
        LOG(FATAL) << "All columns must have the same chunk sizes";
      }
    }
  }
  std::vector<std::shared_ptr<arrow::Field>> fields;
  for (size_t i = 0; i < col_names.size(); ++i) {
    fields.push_back(arrow::field(col_names[i], arrays[i][0]->type()));
  }
  auto schema = arrow::schema(fields);
  for (size_t i = 0; i < chunk_sizes.size(); ++i) {
    std::vector<std::shared_ptr<arrow::Array>> cols;
    for (size_t j = 0; j < arrays.size(); ++j) {
      cols.push_back(arrays[j][i]);
    }
    auto batch =
        arrow::RecordBatch::Make(schema, chunk_sizes[i], std::move(cols));
    batches.push_back(batch);
  }
  return batches;
}

template <typename ID_T>
inline std::vector<ID_T> generate_random_vertices(ID_T vnum, size_t len) {
  std::vector<ID_T> vertices;
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<ID_T> dis(0, vnum - 1);
  for (size_t i = 0; i < len; ++i) {
    vertices.push_back(dis(gen));
  }
  return vertices;
}

template <typename EDATA_T>
std::vector<std::tuple<neug::vid_t, neug::vid_t, EDATA_T>>
generate_random_edges(neug::vid_t src_num, neug::vid_t dst_num, size_t len,
                      bool single = false) {
  std::vector<std::tuple<neug::vid_t, neug::vid_t, EDATA_T>> edges;
  if (!single) {
    auto src_list = generate_random_vertices<neug::vid_t>(src_num, len);
    auto dst_list = generate_random_vertices<neug::vid_t>(dst_num, len);
    auto data_list = generate_random_data<EDATA_T>(len);
    for (size_t i = 0; i < len; ++i) {
      edges.emplace_back(src_list[i], dst_list[i], data_list[i]);
    }
  } else {
    len = std::min(len, static_cast<size_t>(src_num));
    auto dst_list = generate_random_vertices<neug::vid_t>(dst_num, len);
    std::vector<neug::vid_t> src_list;
    for (neug::vid_t i = 0; i < src_num; ++i) {
      src_list.push_back(i);
    }
    std::shuffle(src_list.begin(), src_list.end(),
                 std::default_random_engine(std::random_device()()));
    src_list.resize(len);
    auto data_list = generate_random_data<EDATA_T>(len);
    for (size_t i = 0; i < len; ++i) {
      edges.emplace_back(src_list[i], dst_list[i], data_list[i]);
    }
  }
  return edges;
}

namespace neug {
namespace test {

inline void load_modern_graph(std::shared_ptr<neug::Connection> conn) {
  const char* csv_dir_ptr = std::getenv("MODERN_GRAPH_DATA_DIR");
  if (csv_dir_ptr == nullptr) {
    throw std::runtime_error(
        "MODERN_GRAPH_DATA_DIR environment variable is not set");
  }
  LOG(INFO) << "CSV data dir: " << csv_dir_ptr;
  std::string csv_dir = csv_dir_ptr;
  auto res = conn->Query(
      "CREATE NODE TABLE person(id INT64, name STRING, age INT64, PRIMARY "
      "KEY(id));");
  EXPECT_TRUE(res) << res.error().ToString();

  {
    auto res = conn->Query(
        "CREATE NODE TABLE software(id INT64, name STRING, lang STRING, "
        "PRIMARY "
        "KEY(id));");
    EXPECT_TRUE(res) << res.error().ToString();
  }
  {
    auto res = conn->Query(
        "CREATE REL TABLE knows(FROM person TO person, weight DOUBLE);");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  {
    auto res = conn->Query(
        "CREATE REL TABLE created(FROM person TO software, weight DOUBLE, "
        "since INT64);");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  {
    auto res = conn->Query("COPY person from \"" + csv_dir + "/person.csv\";");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  {
    auto res =
        conn->Query("COPY software from \"" + csv_dir + "/software.csv\";");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  {
    auto res = conn->Query(
        "COPY knows from \"" + csv_dir +
        "/person_knows_person.csv\" (from=\"person\", to=\"person\");");
    EXPECT_TRUE(res) << res.error().ToString();
  }

  {
    auto res = conn->Query("COPY created from \"" + csv_dir +
                           "/person_created_software.csv\" (from=\"person\", "
                           "to =\"software\");");
    EXPECT_TRUE(res) << res.error().ToString();
  }
}
}  // namespace test
}  // namespace neug

#endif  // NEUG_TESTS_UNITTTEST_STORAGES_UTILS_H_