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

#include "neug/storages/loader/loader_utils.h"

#include <glog/logging.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include <fast_float.h>
#include <algorithm>
#include <cctype>
#include <charconv>
#include <cstring>
#include <fstream>
#include <limits>
#include <memory>
#include <mutex>
#include <ostream>
#include <shared_mutex>
#include <sstream>
#include <string_view>
#include <system_error>
#include <thread>
#include <type_traits>
#include <unordered_set>
#include <vector>

#include "csv.hpp"
#include "neug/common/columns/columns_utils.h"
#include "neug/common/columns/value_columns.h"
#include "neug/common/types/value.h"
#include "neug/utils/datetime_parsers.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/property/column.h"
#include "neug/utils/string_utils.h"

namespace neug {

namespace {

constexpr size_t kDefaultCsvChunkRows = 4096;
constexpr size_t kMaxCsvChunkRows = 65536;

size_t resolve_chunk_size(const CsvReadConfig& config) {
  if (config.chunk_size <= 0) {
    return kDefaultCsvChunkRows;
  }
  return static_cast<size_t>(
      std::clamp<int64_t>(config.chunk_size, 1, kMaxCsvChunkRows));
}

csv::CSVFormat build_csv_format(const CsvReadConfig& config) {
  csv::CSVFormat csv_format;
  csv_format.delimiter(config.delimiter);
  if (config.quoting) {
    csv_format.quote(config.quote_char);
    if (config.escaping) {
      LOG_FIRST_N(WARNING, 1)
          << "Both escaping and quoting are enabled for CSV parsing. "
             "Escape characters inside quoted fields may not be handled "
             "correctly by the underlying CSV parser.";
    }
  } else {
    csv_format.quote(false);
  }
  csv_format.variable_columns(csv::VariableColumnPolicy::KEEP);
  csv_format.no_header();
  csv_format.column_names(config.column_names);
  return csv_format;
}

std::unordered_set<std::string> to_lookup_set(
    const std::vector<std::string>& values) {
  std::unordered_set<std::string> lookup_set;
  lookup_set.reserve(values.size());
  for (const auto& value : values) {
    lookup_set.insert(value);
  }
  return lookup_set;
}

std::vector<std::string> resolve_selected_column_names(
    const CsvReadConfig& config) {
  if (!config.include_columns.empty()) {
    return config.include_columns;
  }
  return config.column_names;
}

std::vector<size_t> resolve_selected_column_indices(
    const std::vector<std::string>& selected_column_names,
    const std::vector<std::string>& all_column_names) {
  std::unordered_map<std::string, size_t> name_to_index;
  name_to_index.reserve(all_column_names.size());
  for (size_t index = 0; index < all_column_names.size(); ++index) {
    name_to_index[all_column_names[index]] = index;
  }

  std::vector<size_t> selected_column_indices;
  selected_column_indices.reserve(selected_column_names.size());
  for (const auto& column_name : selected_column_names) {
    auto iter = name_to_index.find(column_name);
    if (iter == name_to_index.end()) {
      THROW_SCHEMA_MISMATCH("Column not found in CSV schema: " + column_name);
    }
    selected_column_indices.push_back(iter->second);
  }
  return selected_column_indices;
}

std::vector<DataType> resolve_selected_column_types(
    const std::vector<std::string>& selected_column_names,
    const CsvReadConfig& config) {
  std::vector<DataType> selected_column_types;
  selected_column_types.reserve(selected_column_names.size());
  for (const auto& column_name : selected_column_names) {
    auto iter = config.column_types.find(column_name);
    if (iter == config.column_types.end()) {
      selected_column_types.emplace_back(DataTypeId::kVarchar);
      continue;
    }
    selected_column_types.push_back(iter->second);
  }
  return selected_column_types;
}

bool contains_sv(const std::unordered_set<std::string>& set,
                 std::string_view sv) {
  for (const auto& v : set) {
    if (v.size() == sv.size() &&
        std::memcmp(v.data(), sv.data(), sv.size()) == 0) {
      return true;
    }
  }
  return false;
}

bool parse_bool_value(std::string_view token,
                      const std::unordered_set<std::string>& true_values,
                      const std::unordered_set<std::string>& false_values) {
  if (token == "1" || token == "true" || token == "True" || token == "TRUE") {
    return true;
  }
  if (token == "0" || token == "false" || token == "False" ||
      token == "FALSE") {
    return false;
  }
  if (contains_sv(true_values, token)) {
    return true;
  }
  if (contains_sv(false_values, token)) {
    return false;
  }
  auto matches_ci = [](std::string_view a, std::string_view b) {
    if (a.size() != b.size())
      return false;
    for (size_t i = 0; i < a.size(); ++i) {
      if (std::tolower(static_cast<unsigned char>(a[i])) !=
          std::tolower(static_cast<unsigned char>(b[i])))
        return false;
    }
    return true;
  };
  if (matches_ci(token, "true"))
    return true;
  if (matches_ci(token, "false"))
    return false;
  THROW_CONVERSION_EXCEPTION("Invalid boolean value: " + std::string(token));
}

std::string unescape_token_sv(std::string_view token, char escape_char) {
  std::string res;
  res.reserve(token.size());
  for (size_t i = 0; i < token.size(); ++i) {
    if (token[i] == escape_char && i + 1 < token.size()) {
      res.push_back(token[i + 1]);
      ++i;
    } else {
      res.push_back(token[i]);
    }
  }
  return res;
}

// Fast path: CSV field → parse_direct<T> → push_back_opt
// Bypasses Value object creation and virtual dispatch (push_back_elem).
// FieldAppender resolves the concrete builder type once per column per chunk.

/// Parse a string_view directly to type T, without wrapping in Value.
template <typename T>
T parse_direct(std::string_view token,
               const std::unordered_set<std::string>& true_values,
               const std::unordered_set<std::string>& false_values) {
  if constexpr (std::is_integral_v<T> && !std::is_same_v<T, bool>) {
    T value{};
    auto [ptr, ec] =
        std::from_chars(token.data(), token.data() + token.size(), value);
    if (ec != std::errc() || ptr != token.data() + token.size()) {
      THROW_CONVERSION_EXCEPTION("Failed to parse numeric value: " +
                                 std::string(token));
    }
    return value;
  } else if constexpr (std::is_floating_point_v<T>) {
    T value{};
    auto result = kuzu_fast_float::from_chars(
        token.data(), token.data() + token.size(), value);
    if (result.ec != std::errc() || result.ptr != token.data() + token.size()) {
      THROW_CONVERSION_EXCEPTION("Failed to parse floating value: " +
                                 std::string(token));
    }
    return value;
  } else if constexpr (std::is_same_v<T, bool>) {
    return parse_bool_value(token, true_values, false_values);
  } else if constexpr (std::is_same_v<T, std::string>) {
    return std::string(token);
  } else if constexpr (std::is_same_v<T, Date>) {
    int64_t millis = 0;
    // Direct call to inline functions — enables full inlining of
    // timestamp parsing logic (no indirection through wrapper functions).
    if (utils::parse_timestamp_ms(token.data(), token.size(), &millis) ||
        utils::parse_epoch_timestamp_ms(token.data(), token.size(), &millis)) {
      Date d;
      d.from_timestamp(millis);
      return d;
    }
    return Date(std::string(token));
  } else if constexpr (std::is_same_v<T, DateTime>) {
    int64_t millis = 0;
    if (utils::parse_timestamp_ms(token.data(), token.size(), &millis) ||
        utils::parse_epoch_timestamp_ms(token.data(), token.size(), &millis)) {
      return DateTime(millis);
    }
    return DateTime(std::string(token));
  } else {
    // Interval and any other types: construct from string.
    return T(std::string(token));
  }
}

/// Shared parsing context (constant across all columns in a chunk).
struct CsvParseCtx {
  const std::unordered_set<std::string>& null_values;
  const std::unordered_set<std::string>& true_values;
  const std::unordered_set<std::string>& false_values;
  const std::string& file_path;
  bool escaping;
  char escape_char;
};

/// Type-erased field appender (32 bytes — fits one cache line).
///
/// Holds a function pointer to a template instantiation that knows the
/// concrete ValueColumnBuilder<T> type.  The function pointer is resolved
/// once per column (in make_appender), then called per-field without
/// virtual dispatch or Value creation.
///
/// column_name and type are only dereferenced in the error path.
struct FieldAppender {
  void* builder;
  const std::string* column_name;
  const DataType* type;
  void (*fn)(const FieldAppender&, csv::CSVField, const CsvParseCtx&, int64_t);

  inline void append(csv::CSVField field, const CsvParseCtx& ctx,
                     int64_t row_number) const {
    fn(*this, field, ctx, row_number);
  }
};

/// Template implementation: parse field → T → push_back_opt.
template <typename T>
void append_typed_impl(const FieldAppender& app, csv::CSVField field,
                       const CsvParseCtx& ctx, int64_t row_number) {
  auto* builder = static_cast<ValueColumnBuilder<T>*>(app.builder);

  if (field.is_null()) {
    builder->push_back_null();
    return;
  }

  std::string_view token_sv = static_cast<csv::string_view>(field);

  if (contains_sv(ctx.null_values, token_sv)) {
    builder->push_back_null();
    return;
  }

  try {
    if (ctx.escaping) {
      auto unescaped = unescape_token_sv(token_sv, ctx.escape_char);
      builder->push_back_opt(
          parse_direct<T>(unescaped, ctx.true_values, ctx.false_values));
    } else {
      builder->push_back_opt(
          parse_direct<T>(token_sv, ctx.true_values, ctx.false_values));
    }
  } catch (const std::exception& error) {
    THROW_CONVERSION_EXCEPTION(
        "Failed to parse CSV field, file=" + ctx.file_path +
        ", row=" + std::to_string(row_number) + ", column=" + *app.column_name +
        ", type=" + app.type->ToString() + ", value='" + std::string(token_sv) +
        "', reason=" + error.what());
  }
}

/// Create a FieldAppender for the given data type.
FieldAppender make_appender(const DataType& type, void* builder,
                            const std::string* column_name) {
  switch (type.id()) {
#define MAKE_APPENDER(enum_val, cpp_type) \
  case DataTypeId::enum_val:              \
    return {builder, column_name, &type, &append_typed_impl<cpp_type>};
    FOR_EACH_DATA_TYPE(MAKE_APPENDER)
#undef MAKE_APPENDER
  default:
    THROW_NOT_SUPPORTED_EXCEPTION("Unsupported data type in CSV parser: " +
                                  type.ToString() + " is not supported");
  }
}

/// Quote-tracking state machine for counting CSV row boundaries.
/// One instance tracks a single speculative execution path (in or out
/// of quotes at chunk start).  Config is set once in init(); step()
/// processes one byte using the stored config.
struct RowCounterState {
  bool in_quotes = false;
  bool has_content = false;
  bool pending_quote = false;
  int64_t count = 0;
  // Config (constant throughout the scan, set in init()).
  bool quoting = false;
  char quote_char = '"';
  bool double_quote = true;
  char delimiter = ',';
  // State: whether the next character starts a new field.
  // Only at field start can a quote_char open a quoted field.
  bool at_field_start = true;

  void init(bool start_in_quotes, bool q, char qc, bool dq, char delim) {
    in_quotes = start_in_quotes;
    has_content = start_in_quotes;  // in quotes → has content
    pending_quote = false;
    count = 0;
    quoting = q;
    quote_char = qc;
    double_quote = dq;
    delimiter = delim;
    at_field_start = !start_in_quotes;  // outside → field start; inside → not
  }

  /// Advance the state machine by one byte.
  void step(char c) {
    if (in_quotes) {
      has_content = true;
      if (pending_quote) {
        if (double_quote && c == quote_char) {
          // "" — escaped quote, stay in quotes.
          pending_quote = false;
          return;
        }
        // Quote ended; fall through to unquoted context.
        in_quotes = false;
        pending_quote = false;
      }
      if (in_quotes) {
        if (c == quote_char) {
          if (double_quote)
            pending_quote = true;
          else
            in_quotes = false;
        }
        return;
      }
      // Quote just closed — not at field start.
      at_field_start = false;
    }
    // Unquoted context.
    // Only treat quote_char as opening quote at field start, matching
    // csv-parser semantics where a bare quote inside an unquoted field
    // is a literal character.
    if (quoting && at_field_start && c == quote_char) {
      in_quotes = true;
      has_content = true;
      at_field_start = false;
    } else if (c == '\n' || c == '\r') {
      // Treat both \n (LF) and \r (CR) as row terminators.
      // For CRLF, \r counts the row and clears has_content,
      // so the following \n is a no-op (no double-count).
      if (has_content)
        ++count;
      has_content = false;
      at_field_start = true;  // new row = new field
    } else if (c == delimiter) {
      at_field_start = true;  // next char starts a new field
    } else {
      has_content = true;
      at_field_start = false;
    }
  }
};

/// Fast CSV row counter: scans raw bytes to count row boundaries
/// without parsing fields.  Uses a quote-tracking state machine;
/// parallelizes via speculative dual-state-machine scan for large files.
class CsvRowCountCounter {
 public:
  // rows_to_skip is intentionally NOT a parameter: the counter counts
  // all non-empty rows.  The reader's skip_rows() handles skipping
  // separately, and RowNum() is only a pre-allocation hint, so a slight
  // overcount (by at most skip_rows, typically 1 for header) is safe.
  CsvRowCountCounter(std::string file_path, bool quoting, char quote_char,
                     bool double_quote, char delimiter)
      : file_path_(std::move(file_path)),
        quoting_(quoting),
        quote_char_(quote_char),
        double_quote_(double_quote),
        delimiter_(delimiter) {}

  int64_t count() const {
    struct stat st;
    if (stat(file_path_.c_str(), &st) != 0) {
      THROW_IO_EXCEPTION("Failed to get file size: " + file_path_);
    }
    auto file_size = static_cast<size_t>(st.st_size);
    if (file_size == 0)
      return 0;

    constexpr size_t kMinChunkSize = 4 << 20;  // 4 MB
    unsigned num_threads = std::thread::hardware_concurrency();
    if (num_threads == 0)
      num_threads = 1;
    if (file_size < kMinChunkSize * num_threads) {
      num_threads =
          std::max(1u, static_cast<unsigned>(file_size / kMinChunkSize));
    }
    if (num_threads <= 1)
      return count_single(file_size);
    return count_parallel(file_size, num_threads);
  }

 private:
  struct ChunkResult {
    RowCounterState outside;  // assumed start outside quotes
    RowCounterState inside;   // assumed start inside quotes
  };

  /// Read [start, end) from the file in 1 MB buffers, calling \p fn
  /// for each buffer.  Shared by count_single() and scan_chunk().
  template <typename Fn>
  void scan_range(size_t start, size_t end, Fn&& fn) const {
    std::ifstream file(file_path_, std::ios::binary);
    if (!file.is_open()) {
      THROW_IO_EXCEPTION("Failed to open file for counting: " + file_path_);
    }
    file.seekg(static_cast<std::streamoff>(start));
    constexpr size_t kBufSize = 1 << 20;  // 1 MB
    std::vector<char> buffer(kBufSize);
    size_t pos = start;
    while (pos < end) {
      size_t to_read = std::min(kBufSize, end - pos);
      file.read(buffer.data(), to_read);
      auto bytes_read = static_cast<size_t>(file.gcount());
      if (bytes_read == 0)
        break;
      fn(buffer.data(), bytes_read);
      pos += bytes_read;
    }
  }

  /// Find byte offset after the first row terminator (\n or \r) at or
  /// after \p start.  Used for newline-aligned chunk boundaries.
  size_t align_to_newline(size_t start, size_t end) const {
    if (start >= end)
      return end;
    std::ifstream file(file_path_, std::ios::binary);
    if (!file.is_open()) {
      THROW_IO_EXCEPTION("Failed to open file for counting: " + file_path_);
    }
    file.seekg(static_cast<std::streamoff>(start));
    if (!file) {
      THROW_IO_EXCEPTION("Failed to seek file for counting: " + file_path_);
    }
    constexpr size_t kScanBuf = 4096;
    char buf[kScanBuf];
    size_t pos = start;
    while (pos < end) {
      size_t to_read = std::min(kScanBuf, end - pos);
      file.read(buf, to_read);
      auto n = static_cast<size_t>(file.gcount());
      if (n == 0)
        break;
      for (size_t i = 0; i < n; ++i) {
        if (buf[i] == '\n' || buf[i] == '\r')
          return pos + i + 1;
      }
      pos += n;
    }
    return end;
  }

  /// Scan a byte range with dual state machines (single pass).
  /// When quoting is disabled, only one state machine is run and the
  /// other is copied — both produce identical results.
  ChunkResult scan_chunk(size_t start, size_t end) const {
    ChunkResult res;
    res.outside.init(false, quoting_, quote_char_, double_quote_, delimiter_);
    res.inside.init(true, quoting_, quote_char_, double_quote_, delimiter_);
    if (quoting_) {
      scan_range(start, end, [&](const char* data, size_t n) {
        for (size_t i = 0; i < n; ++i) {
          char c = data[i];
          res.outside.step(c);
          res.inside.step(c);
        }
      });
    } else {
      // quoting=false: both state machines produce identical results.
      // Only run one to halve the CPU work.
      scan_range(start, end, [&](const char* data, size_t n) {
        for (size_t i = 0; i < n; ++i) {
          res.outside.step(data[i]);
        }
      });
      res.inside = res.outside;
    }
    return res;
  }

  /// Single-threaded scan.
  int64_t count_single(size_t file_size) const {
    RowCounterState state;
    state.init(false, quoting_, quote_char_, double_quote_, delimiter_);
    scan_range(0, file_size, [&](const char* data, size_t n) {
      for (size_t i = 0; i < n; ++i) {
        state.step(data[i]);
      }
    });
    int64_t total = state.count;
    if (state.has_content)
      ++total;  // last row without trailing newline
    return total;
  }

  /// Parallel scan with speculative dual state machines.
  int64_t count_parallel(size_t file_size, unsigned num_threads) const {
    // Compute newline-aligned chunk boundaries.
    size_t approx_chunk = file_size / num_threads;
    std::vector<size_t> bounds;
    bounds.push_back(0);
    for (unsigned i = 1; i < num_threads; ++i) {
      size_t actual = align_to_newline(i * approx_chunk, file_size);
      if (actual <= bounds.back() || actual >= file_size)
        break;
      bounds.push_back(actual);
    }
    bounds.push_back(file_size);
    unsigned actual_threads = static_cast<unsigned>(bounds.size() - 1);

    // Parallel scan: each thread scans its chunk with dual state machines.
    std::vector<ChunkResult> results(actual_threads);
    std::vector<std::thread> threads;
    for (unsigned i = 0; i < actual_threads; ++i) {
      threads.emplace_back([this, &results, i, &bounds]() {
        results[i] = scan_chunk(bounds[i], bounds[i + 1]);
      });
    }
    for (auto& t : threads)
      t.join();

    // Sequential resolution: chain quote state across chunks.
    int64_t total = 0;
    bool in_quotes = false;  // chunk 0 starts outside quotes
    const RowCounterState* last_selected = nullptr;
    for (unsigned i = 0; i < actual_threads; ++i) {
      const RowCounterState& s =
          in_quotes ? results[i].inside : results[i].outside;
      total += s.count;
      in_quotes = s.in_quotes;
      last_selected = &s;
    }

    // Handle last row without trailing newline.
    // Use the actually-selected variant for the last chunk, not the
    // ending in_quotes state (which may differ from the start assumption).
    if (last_selected && last_selected->has_content)
      ++total;

    return total;
  }

  std::string file_path_;
  bool quoting_;
  char quote_char_;
  bool double_quote_;
  char delimiter_;
};

}  // namespace

struct CsvSupplierRuntime {
  explicit CsvSupplierRuntime(const std::string& file_path,
                              const CsvReadConfig& config)
      : file_path_(file_path),
        csv_format_(build_csv_format(config)),
        selected_column_names_(resolve_selected_column_names(config)),
        selected_column_indices_(resolve_selected_column_indices(
            selected_column_names_, config.column_names)),
        selected_column_types_(
            resolve_selected_column_types(selected_column_names_, config)),
        null_values_(to_lookup_set(config.null_values)),
        true_values_(to_lookup_set(config.true_values)),
        false_values_(to_lookup_set(config.false_values)),
        rows_to_skip_(std::max<int64_t>(0, config.skip_rows)),
        chunk_size_(resolve_chunk_size(config)),
        escaping_(config.escaping),
        escape_char_(config.escape_char) {
    if (selected_column_indices_.empty()) {
      THROW_SCHEMA_MISMATCH("No columns selected for CSV file: " + file_path_);
    }
    row_num_ = CsvRowCountCounter(file_path, config.quoting, config.quote_char,
                                  config.double_quote, config.delimiter)
                   .count();
    reset_reader();
  }

  std::shared_ptr<DataChunk> get_next_chunk() {
    if (!reader_) {
      return nullptr;
    }

    std::vector<std::shared_ptr<IContextColumnBuilder>> builders;
    builders.reserve(selected_column_types_.size());
    for (const auto& column_type : selected_column_types_) {
      auto builder = ColumnsUtils::create_builder(column_type);
      builder->reserve(chunk_size_);
      builders.emplace_back(std::move(builder));
    }

    // Pre-resolve typed appenders for each column (once per chunk).
    // This eliminates per-field Value creation and virtual dispatch.
    CsvParseCtx ctx{null_values_, true_values_, false_values_,
                    file_path_,   escaping_,    escape_char_};
    std::vector<FieldAppender> appenders;
    appenders.reserve(builders.size());
    for (size_t i = 0; i < builders.size(); ++i) {
      appenders.push_back(make_appender(selected_column_types_[i],
                                        builders[i].get(),
                                        &selected_column_names_[i]));
    }

    csv::CSVRow row;
    size_t output_rows = 0;
    while (output_rows < chunk_size_ && reader_->read_row(row)) {
      if (row.empty()) {
        continue;
      }
      current_row_number_ += 1;

      for (size_t column_index = 0; column_index < builders.size();
           ++column_index) {
        auto physical_index = selected_column_indices_[column_index];
        if (physical_index >= row.size()) {
          builders[column_index]->push_back_null();
          continue;
        }
        appenders[column_index].append(row[physical_index], ctx,
                                       current_row_number_);
      }
      output_rows += 1;
    }

    if (output_rows == 0) {
      return nullptr;
    }

    auto chunk = std::make_shared<DataChunk>();
    for (size_t column_index = 0; column_index < builders.size();
         ++column_index) {
      chunk->set(static_cast<int>(column_index),
                 builders[column_index]->finish());
    }
    return chunk;
  }

  int64_t row_num() const { return row_num_; }

 private:
  void reset_reader() {
    try {
      reader_ = std::make_unique<csv::CSVReader>(file_path_, csv_format_);
      skip_rows(*reader_);
      current_row_number_ = rows_to_skip_;
    } catch (const std::exception& error) {
      THROW_IO_EXCEPTION("Failed to initialize CSV reader for file: " +
                         file_path_ + ", reason=" + error.what());
    }
  }

  void skip_rows(csv::CSVReader& reader) const {
    csv::CSVRow skipped_row;
    for (int64_t index = 0; index < rows_to_skip_; ++index) {
      if (!reader.read_row(skipped_row)) {
        break;
      }
    }
  }

 private:
  std::string file_path_;
  csv::CSVFormat csv_format_;
  std::vector<std::string> selected_column_names_;
  std::vector<size_t> selected_column_indices_;
  std::vector<DataType> selected_column_types_;
  std::unordered_set<std::string> null_values_;
  std::unordered_set<std::string> true_values_;
  std::unordered_set<std::string> false_values_;
  int64_t rows_to_skip_ = 0;
  size_t chunk_size_ = kDefaultCsvChunkRows;
  bool escaping_ = false;
  char escape_char_ = '\\';
  int64_t row_num_ = 0;
  int64_t current_row_number_ = 0;
  std::unique_ptr<csv::CSVReader> reader_;
};

static bool put_skip_rows_option(const LoadingConfig& loading_config,
                                 CsvReadConfig& config) {
  bool header_row = loading_config.GetHasHeaderRow();
  config.skip_rows = header_row ? 1 : 0;
  return header_row;
}

static void put_escape_char_option(const LoadingConfig& loading_config,
                                   CsvReadConfig& config) {
  config.escaping = loading_config.GetIsEscaping();
  if (config.escaping) {
    config.escape_char = loading_config.GetEscapeChar();
  }
}

static void put_block_size_option(const LoadingConfig& loading_config,
                                  CsvReadConfig& config) {
  auto batch_size = loading_config.GetBatchSize();
  if (batch_size <= 0) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Block size should be positive");
  }
  config.chunk_size = batch_size;
}

static void put_quote_char_option(const LoadingConfig& loading_config,
                                  CsvReadConfig& config) {
  config.quoting = loading_config.GetIsQuoting();
  if (config.quoting) {
    config.quote_char = loading_config.GetQuotingChar();
  }
  config.double_quote = loading_config.GetIsDoubleQuoting();
}

static void put_null_values(const LoadingConfig& loading_config,
                            CsvReadConfig& config) {
  config.null_values = loading_config.GetNullValues();
}

void printDiskRemaining(const std::string& path) {
  struct statvfs buf;
  if (statvfs(path.c_str(), &buf) == 0) {
    LOG(INFO) << "Disk remaining: " << buf.f_bsize * buf.f_bavail / 1024 / 1024
              << "MB";
  }
}

void put_delimiter_option(const std::string& delimiter_str,
                          CsvReadConfig& config) {
  if (delimiter_str.size() != 1 && delimiter_str[0] != '\\') {
    THROW_INVALID_ARGUMENT_EXCEPTION(
        "Delimiter should be a single character, or a escape "
        "character, like '\\t'");
  }
  if (delimiter_str[0] == '\\') {
    if (delimiter_str.size() != 2) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "Delimiter should be a single character");
    }
    switch (delimiter_str[1]) {
    case 't':
      config.delimiter = '\t';
      break;
    default:
      THROW_INVALID_ARGUMENT_EXCEPTION("Unsupported escape character: " +
                                       std::string(1, delimiter_str[1]));
    }
  } else {
    config.delimiter = delimiter_str[0];
  }
}

void put_boolean_option(CsvReadConfig& config) {
  config.true_values = {"True", "true", "TRUE"};
  config.false_values = {"False", "false", "FALSE"};
}

std::string process_header_row_token(const std::string& token, bool is_quoting,
                                     char quote_char, bool is_escaping,
                                     char escape_char) {
  std::string new_token = token;
  // trim the quote char at the beginning and end of the token
  if (is_quoting) {
    if (token.size() >= 2 && token[0] == quote_char &&
        token[token.size() - 1] == quote_char) {
      new_token = token.substr(1, token.size() - 2);
    }
  }
  // unescape the token
  if (is_escaping) {
    std::string res;
    for (size_t i = 0; i < new_token.size(); ++i) {
      if (new_token[i] == escape_char) {
        if (i + 1 < new_token.size()) {
          res.push_back(new_token[i + 1]);
          i++;
        }
      } else {
        res.push_back(new_token[i]);
      }
    }
    new_token = res;
  }
  return new_token;
}

std::vector<std::string> read_header_manual(const std::string& file_name,
                                            char delimiter, bool is_quoting,
                                            char quote_char, bool is_escaping,
                                            char escape_char) {
  std::vector<std::string> res_vec;
  std::ifstream file(file_name);
  std::string line;
  if (file.is_open()) {
    if (std::getline(file, line)) {
      std::stringstream ss(line);
      std::string token;
      while (std::getline(ss, token, delimiter)) {
        size_t endpos = token.find_last_not_of(" \n\r\t");
        if (endpos == std::string::npos) {
          token.clear();
        } else {
          token.erase(endpos + 1);
        }
        token = process_header_row_token(token, is_quoting, quote_char,
                                         is_escaping, escape_char);
        res_vec.push_back(token);
      }
    } else {
      file.close();
      THROW_IO_EXCEPTION("Fail to read header line of file: " + file_name);
    }
    file.close();
  } else {
    THROW_IO_EXCEPTION("Fail to open file: " + file_name);
  }
  return res_vec;
}

std::vector<std::string> read_header(const std::string& file_name,
                                     const CsvReadConfig& config) {
  if (config.escaping) {
    return read_header_manual(file_name, config.delimiter, config.quoting,
                              config.quote_char, config.escaping,
                              config.escape_char);
  }
  try {
    csv::CSVFormat csv_format;
    csv_format.delimiter(config.delimiter);
    if (config.quoting) {
      csv_format.quote(config.quote_char);
    } else {
      csv_format.quote(false);
    }
    csv_format.no_header();
    csv::CSVReader reader(file_name, csv_format);
    csv::CSVRow row;
    if (!reader.read_row(row)) {
      THROW_IO_EXCEPTION("Fail to read header line of file: " + file_name);
    }
    std::vector<std::string> res_vec;
    res_vec.reserve(row.size());
    for (size_t i = 0; i < row.size(); ++i) {
      res_vec.push_back(row[i].get<std::string>());
    }
    return res_vec;
  } catch (const std::exception& error) {
    THROW_IO_EXCEPTION("Fail to read header line of file: " + file_name +
                       ", reason=" + error.what());
  }
}

void deduplicate_column_names(std::vector<std::string>& all_column_names) {
  std::unordered_map<std::string, int> name_count;
  for (auto& name : all_column_names) {
    if (name_count.find(name) == name_count.end()) {
      name_count[name] = 1;
    } else {
      name_count[name]++;
    }
  }
  for (size_t i = 0; i < all_column_names.size(); ++i) {
    auto& name = all_column_names[i];
    if (name_count[name] > 1) {
      auto cur_cnt = name_count[name];
      name_count[name] -= 1;
      all_column_names[i] = name + "_" + std::to_string(cur_cnt);
    }
  }
}

void put_column_names_option(bool header_row, const std::string& file_path,
                             CsvReadConfig& config, size_t len) {
  if (header_row) {
    config.column_names = read_header(file_path, config);
    deduplicate_column_names(config.column_names);
  } else {
    config.column_names.resize(len);
    for (size_t i = 0; i < config.column_names.size(); ++i) {
      config.column_names[i] = std::string("f") + std::to_string(i);
    }
  }
  VLOG(10) << "Got all column names: " << config.column_names.size()
           << neug::to_string(config.column_names);
}

void set_column_types_on_config(const std::vector<DataType>& column_types,
                                const std::vector<std::string>& column_names,
                                CsvReadConfig& config) {
  if (column_types.size() != column_names.size()) {
    THROW_RUNTIME_ERROR("Column types size does not match column names size: " +
                        std::to_string(column_types.size()) + " vs " +
                        std::to_string(column_names.size()));
  }
  for (size_t i = 0; i < column_types.size(); ++i) {
    const auto& col_name = column_names[i];
    if (config.column_types.find(col_name) != config.column_types.end()) {
      THROW_RUNTIME_ERROR("Duplicate column name found: " + col_name);
    }
    config.column_types.insert({col_name, column_types[i]});
  }
}

CsvReadConfig build_csv_read_config(
    const std::string& file_path,
    const std::unordered_map<std::string, std::string>& csv_options,
    const std::vector<DataType>& column_types) {
  CsvReadConfig config;
  put_boolean_option(config);

  static constexpr const char* kDefaultDelimiter = "|";
  if (csv_options.count("DELIMITER")) {
    put_delimiter_option(csv_options.at("DELIMITER"), config);
  } else if (csv_options.count("DELIM")) {
    put_delimiter_option(csv_options.at("DELIM"), config);
  } else {
    put_delimiter_option(kDefaultDelimiter, config);
  }

  if (csv_options.count("ESCAPE")) {
    if (csv_options.at("ESCAPE").size() == 1) {
      config.escaping = true;
      config.escape_char = csv_options.at("ESCAPE")[0];
    } else {
      config.escaping = false;
    }
  }

  if (csv_options.count("QUOTE")) {
    if (csv_options.at("QUOTE").size() == 1) {
      config.quoting = true;
      config.double_quote = false;
      config.quote_char = csv_options.at("QUOTE")[0];
    } else {
      config.quoting = false;
    }
  }

  if (csv_options.count("DOUBLE_QUOTE")) {
    if (!config.quoting) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "CSV quoting must be enabled for double quotes");
    }
    auto value = csv_options.at("DOUBLE_QUOTE");
    config.double_quote = (value == "true" || value == "1" || value == "TRUE");
  }

  bool header_row = true;
  if (csv_options.count("HEADER")) {
    auto val = to_lower_copy(csv_options.at("HEADER"));
    if (val == "false" || val == "0") {
      header_row = false;
    }
  }

  put_column_names_option(header_row, file_path, config, column_types.size());
  if (config.column_names.size() != column_types.size()) {
    THROW_SCHEMA_MISMATCH("Schema mismatch: column names size (" +
                          std::to_string(config.column_names.size()) +
                          ") does not match column types size (" +
                          std::to_string(column_types.size()) + ")");
  }
  set_column_types_on_config(column_types, config.column_names, config);
  config.include_columns = config.column_names;
  if (header_row) {
    config.skip_rows = 1;
  }
  return config;
}

std::vector<std::string> columnMappingsToSelectedCols(
    const std::vector<std::tuple<size_t, std::string, std::string>>&
        column_mappings) {
  std::vector<std::string> selected_cols;
  for (auto& column_mapping : column_mappings) {
    selected_cols.push_back(std::get<1>(column_mapping));
  }
  return selected_cols;
}

CSVChunkSupplier::CSVChunkSupplier(const std::string& file_path,
                                   CsvReadConfig config)
    : file_path_(file_path) {
  runtime_ = std::make_unique<CsvSupplierRuntime>(file_path, config);
  row_num_ = runtime_->row_num();
  VLOG(10) << "Finish init CSVChunkSupplier for file: " << file_path_;
}

CSVChunkSupplier::~CSVChunkSupplier() = default;

std::shared_ptr<DataChunk> CSVChunkSupplier::GetNextChunk() {
  if (!runtime_) {
    THROW_IO_EXCEPTION("CSV runtime is null for file: " + file_path_);
  }
  return runtime_->get_next_chunk();
}

void fillVertexReaderMeta(
    label_t v_label, const std::string& v_label_name, const std::string& v_file,
    const LoadingConfig& loading_config,
    const std::vector<std::string>& vertex_property_names,
    const std::vector<DataTypeId>& vertex_edge_property_types,
    DataTypeId pk_type, const std::string& pk_name, size_t pk_ind,
    CsvReadConfig& config) {
  CHECK(vertex_edge_property_types.size() == vertex_property_names.size());
  put_boolean_option(config);

  put_delimiter_option(loading_config.GetDelimiter(), config);
  bool header_row = put_skip_rows_option(loading_config, config);
  put_escape_char_option(loading_config, config);
  put_quote_char_option(loading_config, config);
  put_block_size_option(loading_config, config);
  put_null_values(loading_config, config);
  put_column_names_option(header_row, v_file, config,
                          vertex_property_names.size() + 1);

  std::vector<std::string> included_col_names;
  std::vector<std::string> mapped_property_names;

  auto cur_label_col_mapping = loading_config.GetVertexColumnMappings(v_label);

  if (cur_label_col_mapping.size() == 0) {
    std::vector<std::string> vertex_property_names_copy = vertex_property_names;
    CHECK(vertex_property_names.size() + 1 == config.column_names.size())
        << " size in schema: " << vertex_property_names.size()
        << ", size in file: " << config.column_names.size() << ","
        << neug::to_string(vertex_property_names)
        << ", read options: " << neug::to_string(config.column_names);
    vertex_property_names_copy.insert(
        vertex_property_names_copy.begin() + pk_ind, pk_name);

    for (size_t i = 0; i < config.column_names.size(); ++i) {
      included_col_names.emplace_back(config.column_names[i]);
      mapped_property_names.emplace_back(vertex_property_names_copy[i]);
    }
  } else {
    for (size_t i = 0; i < cur_label_col_mapping.size(); ++i) {
      auto& [col_id, col_name, property_name] = cur_label_col_mapping[i];
      if (col_name.empty()) {
        if (col_id >= config.column_names.size() || col_id < 0) {
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "The specified column index: " + std::to_string(col_id) +
              " is out of range, please check your configuration");
        }
        col_name = config.column_names[col_id];
      }
      if (col_id >= 0 &&
          col_id < static_cast<int64_t>(config.column_names.size())) {
        if (col_name != config.column_names[col_id]) {
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "The specified column name: " + col_name +
              " does not match the column name in the file: " +
              config.column_names[col_id]);
        }
      }
      included_col_names.emplace_back(col_name);
      mapped_property_names.emplace_back(property_name);
    }
  }

  VLOG(10) << "Include columns: " << included_col_names.size();
  config.include_columns = included_col_names;

  for (size_t i = 0; i < vertex_edge_property_types.size(); ++i) {
    auto property_type = vertex_edge_property_types[i];
    auto property_name = vertex_property_names[i];
    size_t ind = mapped_property_names.size();
    for (size_t j = 0; j < mapped_property_names.size(); ++j) {
      if (mapped_property_names[j] == property_name) {
        ind = j;
        break;
      }
    }
    if (ind == mapped_property_names.size()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "The specified property name: " + property_name +
          " does not exist in the vertex column mapping for "
          "vertex label: " +
          v_label_name +
          " please "
          "check your configuration");
    }
    VLOG(10) << "vertex_label: " << v_label_name
             << " property_name: " << property_name
             << " property_type: " << property_type << " ind: " << ind;
    config.column_types.insert(
        {included_col_names[ind], DataType(property_type)});
  }
  {
    size_t ind = mapped_property_names.size();
    for (size_t i = 0; i < mapped_property_names.size(); ++i) {
      if (mapped_property_names[i] == pk_name) {
        ind = i;
        break;
      }
    }
    if (ind == mapped_property_names.size()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "The specified property name: " + pk_name +
          " does not exist in the vertex column mapping, please "
          "check your configuration");
    }
    config.column_types.insert({included_col_names[ind], DataType(pk_type)});
  }
}

void fillEdgeReaderMeta(label_t src_label_id, label_t dst_label_id,
                        label_t label_id, const std::string& edge_label_name,
                        const std::string& e_file,
                        const LoadingConfig& loading_config,
                        const std::vector<std::string>& edge_property_names,
                        const std::vector<DataTypeId>& edge_property_types,
                        DataTypeId src_pk_type, DataTypeId dst_pk_type,
                        CsvReadConfig& config) {
  CHECK(edge_property_types.size() == edge_property_names.size());
  put_boolean_option(config);

  put_delimiter_option(loading_config.GetDelimiter(), config);
  bool header_row = put_skip_rows_option(loading_config, config);
  put_escape_char_option(loading_config, config);
  put_quote_char_option(loading_config, config);
  put_block_size_option(loading_config, config);
  put_null_values(loading_config, config);
  put_column_names_option(header_row, e_file, config,
                          edge_property_names.size() + 2);

  auto src_dst_cols =
      loading_config.GetEdgeSrcDstCol(src_label_id, dst_label_id, label_id);

  std::vector<std::string> included_col_names;
  std::vector<std::string> mapped_property_names;

  {
    CHECK(src_dst_cols.first.size() == 1 && src_dst_cols.second.size() == 1);
    auto src_col_ind = src_dst_cols.first[0].second;
    auto dst_col_ind = src_dst_cols.second[0].second;
    CHECK(src_col_ind >= 0 && src_col_ind < config.column_names.size())
        << " src_col_ind: " << src_col_ind
        << ", column_names.size(): " << config.column_names.size();
    CHECK(dst_col_ind >= 0 && dst_col_ind < config.column_names.size())
        << " dst_col_ind: " << dst_col_ind
        << ", column_names.size(): " << config.column_names.size();

    included_col_names.emplace_back(config.column_names[src_col_ind]);
    included_col_names.emplace_back(config.column_names[dst_col_ind]);
  }

  auto cur_label_col_mapping = loading_config.GetEdgeColumnMappings(
      src_label_id, dst_label_id, label_id);
  if (cur_label_col_mapping.empty()) {
    for (size_t i = 0; i < edge_property_names.size(); ++i) {
      auto property_name = edge_property_names[i];
      if (loading_config.GetHasHeaderRow()) {
        included_col_names.emplace_back(property_name);
      } else {
        included_col_names.emplace_back(config.column_names[i + 2]);
      }
      mapped_property_names.emplace_back(property_name);
    }
  } else {
    for (size_t i = 0; i < cur_label_col_mapping.size(); ++i) {
      auto& [col_id, col_name, property_name] = cur_label_col_mapping[i];
      if (col_name.empty()) {
        if (col_id >= config.column_names.size() || col_id < 0) {
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "The specified column index: " + std::to_string(col_id) +
              " is out of range, please check your configuration");
        }
        col_name = config.column_names[col_id];
      }
      if (col_id >= 0 &&
          col_id < static_cast<int64_t>(config.column_names.size())) {
        if (col_name != config.column_names[col_id]) {
          THROW_INVALID_ARGUMENT_EXCEPTION(
              "The specified column name: " + col_name +
              " does not match the column name in the file: " +
              config.column_names[col_id]);
        }
      }
      if (loading_config.GetHasHeaderRow()) {
        included_col_names.emplace_back(col_name);
      } else {
        included_col_names.emplace_back(config.column_names[col_id]);
      }
      mapped_property_names.emplace_back(property_name);
    }
  }

  VLOG(10) << "Include Edge columns: " << neug::to_string(included_col_names);
  config.include_columns = included_col_names;

  for (size_t i = 0; i < edge_property_types.size(); ++i) {
    auto property_type = edge_property_types[i];
    auto property_name = edge_property_names[i];
    size_t ind = mapped_property_names.size();
    for (size_t j = 0; j < mapped_property_names.size(); ++j) {
      if (mapped_property_names[j] == property_name) {
        ind = j;
        break;
      }
    }
    if (ind == mapped_property_names.size()) {
      THROW_INVALID_ARGUMENT_EXCEPTION(
          "The specified property name: " + property_name +
          " does not exist in the vertex column mapping, please "
          "check your configuration");
    }
    VLOG(10) << "edge_label: " << edge_label_name
             << " property_name: " << property_name
             << " property_type: " << property_type << " ind: " << ind;
    config.column_types.insert(
        {included_col_names[ind + 2], DataType(property_type)});
  }
  {
    auto src_dst_cols =
        loading_config.GetEdgeSrcDstCol(src_label_id, dst_label_id, label_id);
    CHECK(src_dst_cols.first.size() == 1 && src_dst_cols.second.size() == 1);
    auto src_col_ind = src_dst_cols.first[0].second;
    auto dst_col_ind = src_dst_cols.second[0].second;
    CHECK(src_col_ind >= 0 &&
          src_col_ind < static_cast<int64_t>(config.column_names.size()));
    CHECK(dst_col_ind >= 0 &&
          dst_col_ind < static_cast<int64_t>(config.column_names.size()));
    config.column_types.insert(
        {config.column_names[src_col_ind], DataType(src_pk_type)});
    config.column_types.insert(
        {config.column_names[dst_col_ind], DataType(dst_pk_type)});

    VLOG(10) << "Column types: ";
    for (const auto& iter : config.column_types) {
      VLOG(10) << iter.first << " : " << iter.second.ToString();
    }
  }
}

namespace {

template <typename SRC_T, typename COL_T = SRC_T>
void set_column_from_value_column(
    ColumnBase* col, const std::shared_ptr<IContextColumn>& ctx_col,
    const std::vector<vid_t>& vids) {
  auto* typed = dynamic_cast<TypedColumn<COL_T>*>(col);
  CHECK(typed != nullptr)
      << "Storage column type does not match expected type.";

  auto value_col = std::dynamic_pointer_cast<ValueColumn<SRC_T>>(ctx_col);

  if constexpr (std::is_same_v<COL_T, std::string_view>) {
    auto write = [&](vid_t vid, const auto& s) {
      if (typed->available_space() <= s.size()) {
        typed->resize(typed->size());
      }
      typed->set_value(vid, std::string_view(s));
    };
    for (size_t k = 0; k < vids.size(); ++k) {
      if (vids[k] >= std::numeric_limits<vid_t>::max())
        continue;
      if (value_col) {
        write(vids[k], value_col->data()[k]);
      } else {
        auto val = ctx_col->get_elem(k);
        if (!val.IsNull())
          write(vids[k], val.GetValue<std::string>());
      }
    }
  } else {
    for (size_t k = 0; k < vids.size(); ++k) {
      if (vids[k] >= std::numeric_limits<vid_t>::max())
        continue;
      if (value_col) {
        typed->set_value(vids[k], value_col->data()[k]);
      } else {
        auto val = ctx_col->get_elem(k);
        if (!val.IsNull())
          typed->set_value(vids[k], val.GetValue<COL_T>());
      }
    }
  }
}

}  // namespace

void set_properties_from_context_column(
    neug::ColumnBase* col, const std::shared_ptr<IContextColumn>& ctx_col,
    const std::vector<vid_t>& vids) {
  auto col_type = col->type();
  switch (col_type) {
#define SET_TYPED_VALUE(enum_val, ctype)                     \
  case DataTypeId::enum_val: {                               \
    set_column_from_value_column<ctype>(col, ctx_col, vids); \
    break;                                                   \
  }
    FOR_EACH_DATA_TYPE_PRIMITIVE(SET_TYPED_VALUE)
#undef SET_TYPED_VALUE
  case DataTypeId::kDate: {
    set_column_from_value_column<Date>(col, ctx_col, vids);
    break;
  }
  case DataTypeId::kTimestampMs: {
    set_column_from_value_column<DateTime>(col, ctx_col, vids);
    break;
  }
  case DataTypeId::kInterval: {
    set_column_from_value_column<Interval>(col, ctx_col, vids);
    break;
  }
  case DataTypeId::kVarchar: {
    set_column_from_value_column<std::string, std::string_view>(col, ctx_col,
                                                                vids);
    break;
  }
  default:
    THROW_NOT_SUPPORTED_EXCEPTION(
        "set_properties_from_context_column: unsupported type " +
        DataType(col_type).ToString());
  }
}

}  // namespace neug
