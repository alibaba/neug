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

#pragma once

#include <algorithm>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/storages/loader/csv_read_config.h"
#include "neug/storages/loader/json_read_config.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/reader/schema.h"

namespace neug {
namespace reader {

static constexpr int64_t kSniffBlockSize = 1 << 20;

struct ReadSharedState;
struct EntrySchema;

using options_t = common::case_insensitive_map_t<std::string>;

template <typename T>
class Option {
 public:
  using ParseFunc = std::function<T(const std::string&)>;

  Option(std::string key, std::string default_val, ParseFunc parse_func)
      : key_(std::move(key)),
        default_val_(std::move(default_val)),
        parse_func_(std::move(parse_func)) {}

  T get(const options_t& options) const {
    auto it = options.find(key_);
    std::string value = (it != options.end()) ? it->second : default_val_;
    return parse_func_(value);
  }

  const std::string& getKey() const { return key_; }

  static Option<int32_t> Int32Option(const std::string& key,
                                     int32_t default_val) {
    return Option<int32_t>(
        key, std::to_string(default_val), [](const std::string& s) -> int32_t {
          try {
            return static_cast<int32_t>(std::stol(s));
          } catch (const std::exception& e) {
            THROW_INVALID_ARGUMENT_EXCEPTION("Failed to parse int: " + s);
          }
        });
  }

  static Option<int64_t> Int64Option(const std::string& key,
                                     int64_t default_val) {
    return Option<int64_t>(
        key, std::to_string(default_val), [](const std::string& s) -> int64_t {
          try {
            return std::stoll(s);
          } catch (const std::exception& e) {
            THROW_INVALID_ARGUMENT_EXCEPTION("Failed to parse int64_t: " + s);
          }
        });
  }

  static Option<std::string> StringOption(const std::string& key,
                                          const std::string& default_val) {
    return Option<std::string>(
        key, default_val,
        [](const std::string& s) -> std::string { return s; });
  }

  static Option<char> CharOption(const std::string& key, char default_val) {
    return Option<char>(key, std::string(1, default_val),
                        [](const std::string& s) -> char { return s[0]; });
  }

  static Option<bool> BoolOption(const std::string& key, bool default_val) {
    return Option<bool>(
        key, default_val ? "true" : "false", [](const std::string& s) -> bool {
          std::string lower = s;
          std::transform(lower.begin(), lower.end(), lower.begin(), ::tolower);
          if (lower == "true" || lower == "1" || lower == "yes" ||
              lower == "on") {
            return true;
          } else if (lower == "false" || lower == "0" || lower == "no" ||
                     lower == "off") {
            return false;
          } else {
            THROW_INVALID_ARGUMENT_EXCEPTION("Invalid boolean value: " + s);
          }
        });
  }

  static Option<double> DoubleOption(const std::string& key,
                                     double default_val) {
    return Option<double>(
        key, std::to_string(default_val), [](const std::string& s) -> double {
          try {
            double val = std::stod(s);
            if (val < 0) {
              THROW_INVALID_ARGUMENT_EXCEPTION(
                  "Value must be non-negative, got: " + s);
            }
            return val;
          } catch (const exception::Exception&) {
            throw;
          } catch (const std::exception& e) {
            THROW_INVALID_ARGUMENT_EXCEPTION("Failed to parse double: " + s);
          }
        });
  }

 private:
  std::string key_;
  std::string default_val_;
  ParseFunc parse_func_;
};

struct CSVParseOptions {
  Option<char> delimiter = Option<char>::CharOption("delim", '|');
  Option<bool> quoting = Option<bool>::BoolOption("quoting", true);
  Option<char> quote_char = Option<char>::CharOption("quote", '"');
  Option<bool> escaping = Option<bool>::BoolOption("escaping", true);
  Option<char> escape_char = Option<char>::CharOption("escape", '\\');
  Option<bool> has_header = Option<bool>::BoolOption("header", true);
};

struct ReadOptions {
  Option<bool> use_threads = Option<bool>::BoolOption("parallel", true);
  Option<bool> batch_read = Option<bool>::BoolOption("batch_read", true);
  Option<int64_t> batch_size =
      Option<int64_t>::Int64Option("batch_size", 1 << 20);
  Option<bool> autogenerate_column_names =
      Option<bool>::BoolOption("autogenerate_column_names", false);
  Option<int32_t> skip_rows = Option<int32_t>::Int32Option("skip_rows", 0);
};

template <class T>
class OptionsBuilder {
 public:
  explicit OptionsBuilder(std::shared_ptr<ReadSharedState> state)
      : state(state) {}
  virtual ~OptionsBuilder() = default;

  virtual T build() const = 0;
  virtual bool projectColumns(T& options) { return false; }
  virtual bool skipRows(T& options) { return false; }

 protected:
  std::shared_ptr<ReadSharedState> state;
};

class CsvOptionsBuilder : public OptionsBuilder<CsvReadConfig> {
 public:
  explicit CsvOptionsBuilder(std::shared_ptr<ReadSharedState> state)
      : OptionsBuilder<CsvReadConfig>(state) {}

  CsvReadConfig build() const override;
  bool projectColumns(CsvReadConfig& config) override;
};

class JsonOptionsBuilder : public OptionsBuilder<JsonReadConfig> {
 public:
  explicit JsonOptionsBuilder(std::shared_ptr<ReadSharedState> state,
                              bool json_array_input = false)
      : OptionsBuilder<JsonReadConfig>(state),
        json_array_input_(json_array_input) {}

  JsonReadConfig build() const override;
  bool projectColumns(JsonReadConfig& config) override;

 private:
  bool json_array_input_;
};

}  // namespace reader
}  // namespace neug
