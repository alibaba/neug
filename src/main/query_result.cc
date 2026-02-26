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

#include "neug/main/query_result.h"

#include <glog/logging.h>
#include <stdint.h>
#include <cstring>
#include <memory>
#include <ostream>
#include <sstream>
#include <string_view>
#include <utility>
#include <vector>
#include "neug/utils/exception/exception.h"
#include "neug/utils/pb_utils.h"

#include <arrow/api.h>
#include <arrow/array/concatenate.h>
#include <arrow/io/api.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/memory_pool.h>
#include <arrow/pretty_print.h>
#include <arrow/scalar.h>
#include <arrow/type.h>

namespace neug {

inline std::vector<char> SerializeArrowIPC(
    const std::shared_ptr<arrow::Table>& table) {
  if (!table || table->num_rows() == 0) {
    return {};
  }

  auto buffer_output_res =
      arrow::io::BufferOutputStream::Create(1024, arrow::default_memory_pool());
  if (!buffer_output_res.ok()) {
    LOG(ERROR) << "Failed to create BufferOutputStream: "
               << buffer_output_res.status().ToString();
    return {};
  }
  auto buffer_output = buffer_output_res.ValueOrDie();

  auto stream_writer_res =
      arrow::ipc::MakeStreamWriter(buffer_output, table->schema());
  if (!stream_writer_res.ok()) {
    LOG(ERROR) << "Failed to create RecordBatchStreamWriter: "
               << stream_writer_res.status().ToString();
    return {};
  }
  auto writer = stream_writer_res.ValueOrDie();
  THROW_IF_ARROW_NOT_OK(writer->WriteTable(*table));
  THROW_IF_ARROW_NOT_OK(writer->Close());
  // TODO(zhanglei): avoid memory copy by directly using the buffer from
  // BufferOutputStream
  auto buffer_res = buffer_output->Finish();
  if (!buffer_res.ok()) {
    LOG(ERROR) << "Failed to finish BufferOutputStream: "
               << buffer_res.status().ToString();
    return {};
  }
  auto buffer = buffer_res.ValueOrDie();
  std::vector<char> result(buffer->size() + 1);
  result[0] = static_cast<char>(SerializationProtocol::kArrowIPC);
  std::memcpy(result.data() + 1, buffer->data(), buffer->size());
  return result;
}

inline QueryResult DeserializeArrowIPC(std::string_view payload) {
  QueryResult ret;
  if (payload.empty()) {
    return ret;
  }
  auto buffer = arrow::Buffer::Wrap(payload.data(), payload.size());
  std::shared_ptr<arrow::io::BufferReader> buffer_reader =
      std::make_shared<arrow::io::BufferReader>(buffer);

  auto reader_res = arrow::ipc::RecordBatchStreamReader::Open(buffer_reader);
  if (!reader_res.ok()) {
    LOG(ERROR) << "Failed to create RecordBatchStreamReader: "
               << reader_res.status().ToString();
    THROW_RUNTIME_ERROR("Failed to create RecordBatchStreamReader");
  }
  auto reader = reader_res.ValueOrDie();
  // std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
  std::vector<arrow::ArrayVector> array_vectors;
  while (true) {
    auto batch_res = reader->Next();
    if (!batch_res.ok()) {
      LOG(ERROR) << "Failed to read next RecordBatch: "
                 << batch_res.status().ToString();
      THROW_RUNTIME_ERROR("Failed to read next RecordBatch");
    }
    auto batch = batch_res.ValueOrDie();
    if (!batch) {
      break;
    }
    // batches.push_back(batch);
    if (array_vectors.empty()) {
      array_vectors.resize(batch->num_columns());
    }
    for (int i = 0; i < batch->num_columns(); ++i) {
      array_vectors[i].push_back(batch->column(i));
    }
  }
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (const auto& array_vector : array_vectors) {
    auto concat_res = arrow::Concatenate(array_vector);
    if (!concat_res.ok()) {
      LOG(ERROR) << "Failed to concatenate arrays: "
                 << concat_res.status().ToString();
      THROW_RUNTIME_ERROR("Failed to concatenate arrays");
    }
    arrays.push_back(concat_res.ValueOrDie());
  }
  return QueryResult(reader->schema()->fields(), arrays);
}

std::string QueryResult::ToString() const {
  auto table_ = table();
  if (!table_ || table_->num_rows() == 0) {
    return "";
  }
  std::stringstream ss;
  arrow::PrettyPrintOptions options;
  options.indent = 2;
  options.window = static_cast<int>(table_->num_rows());
  auto status = arrow::PrettyPrint(*table_, options, &ss);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to pretty print query result: " << status.ToString();
    return "";
  }
  return ss.str();
}

std::vector<char> QueryResult::Serialize() const {
  if (arrays_.empty()) {
    return {};
  }

  if (serialization_protocol_ == SerializationProtocol::kArrowIPC) {
    auto table_ = table();
    if (!table_) {
      return {};
    }
    return SerializeArrowIPC(table_);
  } else {
    THROW_NOT_IMPLEMENTED_EXCEPTION(
        "Serialization protocol not supported: " +
        std::to_string(static_cast<int>(serialization_protocol_)));
    return {};
  }
}

QueryResult QueryResult::From(const std::string& serialized_table) {
  return From(std::string(serialized_table));
}

QueryResult QueryResult::From(std::string&& serialized_table) {
  if (serialized_table.empty()) {
    LOG(INFO) << "Deserialized empty QueryResult.";
    return QueryResult();
  }
  int first_byte = static_cast<unsigned char>(serialized_table[0]);
  std::string_view rel_payload(serialized_table.data() + 1,
                               serialized_table.size() - 1);
  SerializationProtocol protocol = ParseSerializationProtocol(first_byte);
  if (protocol == SerializationProtocol::kArrowIPC) {
    auto result = DeserializeArrowIPC(rel_payload);
    VLOG(10) << "Deserialized Arrow Table with " << result.length()
             << " rows and "
             << (result.table() ? result.table()->num_columns() : 0)
             << " columns.";
    return result;
  } else {
    LOG(ERROR) << "Unsupported serialization protocol: "
               << static_cast<int>(protocol);
    THROW_NOT_IMPLEMENTED_EXCEPTION("Serialization protocol not supported: " +
                                    std::to_string(static_cast<int>(protocol)));
    return QueryResult();
  }
}

void QueryResult::set_result_schema(const std::string& schema) {
  auto column_names = parse_result_schema_column_names(schema);
  if (fields_.size() != column_names.size()) {
    LOG(WARNING) << "Result schema column count does not match the table "
                    "column count.";
    return;
  }
  for (size_t i = 0; i < fields_.size(); ++i) {
    fields_[i] = arrow::field(column_names[i], fields_[i]->type(),
                              fields_[i]->nullable());
  }
}

}  // namespace neug
