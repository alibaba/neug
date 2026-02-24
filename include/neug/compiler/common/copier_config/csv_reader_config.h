/**
 * Copyright 2020 Alibaba Group Holding Limited.
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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include "neug/compiler/common/case_insensitive_map.h"
#include "neug/compiler/common/constants.h"
#include "neug/compiler/common/copy_constructors.h"
#include "neug/compiler/common/types/value/value.h"

namespace neug {
namespace common {

struct CSVOption {
  // TODO(Xiyang): Add newline character option and delimiter can be a string.
  char escapeChar;
  char delimiter;
  char quoteChar;
  bool hasHeader;
  uint64_t skipNum;
  uint64_t sampleSize;
  bool allowUnbracedList;
  bool ignoreErrors;

  bool autoDetection;
  // These fields aim to identify whether the options are set by user, or set by
  // default.
  bool setEscape;
  bool setDelim;
  bool setQuote;
  bool setHeader;

  CSVOption()
      : escapeChar{CopyConstants::DEFAULT_CSV_ESCAPE_CHAR},
        delimiter{CopyConstants::DEFAULT_CSV_DELIMITER},
        quoteChar{CopyConstants::DEFAULT_CSV_QUOTE_CHAR},
        hasHeader{CopyConstants::DEFAULT_CSV_HAS_HEADER},
        skipNum{CopyConstants::DEFAULT_CSV_SKIP_NUM},
        sampleSize{CopyConstants::DEFAULT_CSV_TYPE_DEDUCTION_SAMPLE_SIZE},
        allowUnbracedList{CopyConstants::DEFAULT_CSV_ALLOW_UNBRACED_LIST},
        ignoreErrors(CopyConstants::DEFAULT_IGNORE_ERRORS),
        autoDetection{CopyConstants::DEFAULT_CSV_AUTO_DETECT},
        setEscape{CopyConstants::DEFAULT_CSV_SET_DIALECT},
        setDelim{CopyConstants::DEFAULT_CSV_SET_DIALECT},
        setQuote{CopyConstants::DEFAULT_CSV_SET_DIALECT},
        setHeader{CopyConstants::DEFAULT_CSV_SET_DIALECT} {}

  EXPLICIT_COPY_DEFAULT_MOVE(CSVOption);

  // TODO: COPY FROM and COPY TO should support transform special options, like
  // '\'.
  std::string toCypher() const {
    std::string result;

    // Add the option IFF option is set by user.
    if (setHeader) {
      std::string header = hasHeader ? "true" : "false";
      result += "header=" + header;
    }
    if (setEscape) {
      if (!result.empty())
        result += ", ";  // Add separator if not the first option
      result += stringFormat("escape='\\{}'", escapeChar);
    }
    if (setDelim) {
      if (!result.empty())
        result += ", ";
      result += stringFormat("delim='{}'", delimiter);
    }
    if (setQuote) {
      if (!result.empty())
        result += ", ";
      result += stringFormat("quote='\\{}'", quoteChar);
    }

    // If no options, return empty string.
    if (result.empty()) {
      return "";
    }

    return "(" + result + ")";
  }

  // Explicit copy constructor
  CSVOption(const CSVOption& other)
      : escapeChar{other.escapeChar},
        delimiter{other.delimiter},
        quoteChar{other.quoteChar},
        hasHeader{other.hasHeader},
        skipNum{other.skipNum},
        sampleSize{
            other.sampleSize == 0
                ? CopyConstants::DEFAULT_CSV_TYPE_DEDUCTION_SAMPLE_SIZE
                : other.sampleSize},  // Set to
                                      // DEFAULT_CSV_TYPE_DEDUCTION_SAMPLE_SIZE
                                      // if sampleSize is 0
        allowUnbracedList{other.allowUnbracedList},
        ignoreErrors{other.ignoreErrors},
        autoDetection{other.autoDetection},
        setEscape{other.setEscape},
        setDelim{other.setDelim},
        setQuote{other.setQuote},
        setHeader{other.setHeader} {}
};

struct CSVReaderConfig {
  CSVOption option;
  bool parallel;

  CSVReaderConfig() : option{}, parallel{CopyConstants::DEFAULT_CSV_PARALLEL} {}
  EXPLICIT_COPY_DEFAULT_MOVE(CSVReaderConfig);

  static CSVReaderConfig construct(
      const case_insensitive_map_t<common::Value>& options);

 private:
  CSVReaderConfig(const CSVReaderConfig& other)
      : option{other.option.copy()}, parallel{other.parallel} {}
};

}  // namespace common
}  // namespace neug
