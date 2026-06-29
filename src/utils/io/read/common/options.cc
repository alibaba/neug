#include "neug/utils/io/read/common/options.h"

#include "neug/compiler/common/assert.h"
#include "neug/storages/loader/loader_utils.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/schema.h"
#include "neug/utils/io/read/common/type_converter.h"
#include "neug/utils/io/reader.h"

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

namespace neug {
namespace reader {

CsvReadConfig CsvOptionsBuilder::build() const {
  if (!state) {
    THROW_INVALID_ARGUMENT_EXCEPTION("State is null");
  }

  const FileSchema& fileSchema = state->schema.file;
  const auto& options = fileSchema.options;
  CSVParseOptions parseOpts;
  ReadOptions readOpts;

  CsvReadConfig config;
  put_boolean_option(config);

  std::string delim_str(1, parseOpts.delimiter.get(options));
  if (options.count("DELIM")) {
    delim_str = options.at("DELIM");
  } else if (options.count("delim")) {
    delim_str = options.at("delim");
  } else if (options.count("DELIMITER")) {
    delim_str = options.at("DELIMITER");
  }
  put_delimiter_option(delim_str, config);

  config.quoting = parseOpts.quoting.get(options);
  config.quote_char = parseOpts.quote_char.get(options);
  config.escaping = parseOpts.escaping.get(options);
  config.escape_char = parseOpts.escape_char.get(options);
  config.skip_rows = readOpts.skip_rows.get(options);

  int64_t batch_size = readOpts.batch_size.get(options);
  if (batch_size <= 0) {
    config.chunk_size = 4096;
  } else {
    config.chunk_size = std::min<int64_t>(batch_size, 65536);
  }

  const EntrySchema* entrySchema = state->schema.entry.get();
  if (readOpts.autogenerate_column_names.get(options)) {
    size_t column_count = 0;
    if (entrySchema && !entrySchema->columnNames.empty()) {
      column_count = entrySchema->columnNames.size();
    } else if (!fileSchema.paths.empty()) {
      CsvReadConfig header_config = config;
      header_config.column_names.clear();
      config.column_names = read_header(fileSchema.paths[0], header_config);
      column_count = config.column_names.size();
    }
    config.column_names.clear();
    config.column_names.reserve(column_count);
    for (size_t i = 0; i < column_count; ++i) {
      config.column_names.push_back("f" + std::to_string(i));
    }
  } else if (entrySchema && !entrySchema->columnNames.empty()) {
    config.column_names = entrySchema->columnNames;
  } else if (!fileSchema.paths.empty()) {
    config.column_names = read_header(fileSchema.paths[0], config);
    if (parseOpts.has_header.get(options) &&
        !readOpts.autogenerate_column_names.get(options) &&
        config.skip_rows == 0) {
      config.skip_rows = 1;
    }
  }

  if (entrySchema && !entrySchema->columnNames.empty() &&
      !entrySchema->columnTypes.empty()) {
    NEUG_ASSERT(entrySchema->columnNames.size() ==
                entrySchema->columnTypes.size());
    NeuGTypeConverter converter;
    for (size_t i = 0; i < entrySchema->columnNames.size(); ++i) {
      config.column_types[entrySchema->columnNames[i]] =
          converter.convert(*entrySchema->columnTypes[i]);
    }
  }

  config.include_columns = config.column_names;
  return config;
}

JsonReadConfig JsonOptionsBuilder::build() const {
  if (!state) {
    THROW_INVALID_ARGUMENT_EXCEPTION("State is null");
  }

  const FileSchema& fileSchema = state->schema.file;
  const auto& options = fileSchema.options;
  ReadOptions readOpts;

  JsonReadConfig config;
  config.newlines_in_values =
      Option<bool>::BoolOption("newlines_in_values", false).get(options);
  config.json_array_input = json_array_input_;

  int64_t batch_size = readOpts.batch_size.get(options);
  if (batch_size <= 0) {
    config.chunk_size = 4096;
  } else {
    config.chunk_size = std::min<int64_t>(batch_size, 65536);
  }

  const EntrySchema* entrySchema = state->schema.entry.get();
  if (entrySchema && !entrySchema->columnNames.empty()) {
    config.column_names = entrySchema->columnNames;
  }

  if (entrySchema && !entrySchema->columnNames.empty() &&
      !entrySchema->columnTypes.empty()) {
    NEUG_ASSERT(entrySchema->columnNames.size() ==
                entrySchema->columnTypes.size());
    NeuGTypeConverter converter;
    for (size_t i = 0; i < entrySchema->columnNames.size(); ++i) {
      config.column_types[entrySchema->columnNames[i]] =
          converter.convert(*entrySchema->columnTypes[i]);
    }
  }

  config.include_columns = config.column_names;
  return config;
}

bool JsonOptionsBuilder::projectColumns(JsonReadConfig& config) {
  if (state->projectColumns.empty()) {
    return true;
  }
  if (!state->schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }
  const EntrySchema& entrySchema = *state->schema.entry;
  for (const auto& column : state->projectColumns) {
    if (std::find(entrySchema.columnNames.begin(),
                  entrySchema.columnNames.end(),
                  column) == entrySchema.columnNames.end()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Column not found in entry schema: " +
                                       column);
    }
  }
  config.include_columns = state->projectColumns;
  return true;
}

bool CsvOptionsBuilder::projectColumns(CsvReadConfig& config) {
  if (state->projectColumns.empty()) {
    return true;
  }
  if (!state->schema.entry) {
    THROW_INVALID_ARGUMENT_EXCEPTION("Entry schema is null");
  }
  const EntrySchema& entrySchema = *state->schema.entry;
  for (const auto& column : state->projectColumns) {
    if (std::find(entrySchema.columnNames.begin(),
                  entrySchema.columnNames.end(),
                  column) == entrySchema.columnNames.end()) {
      THROW_INVALID_ARGUMENT_EXCEPTION("Column not found in entry schema: " +
                                       column);
    }
  }
  config.include_columns = state->projectColumns;
  return true;
}
}  // namespace reader
}  // namespace neug
