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

#include <carquet/carquet.h>

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int validate_roundtrip(const char* name,
                              carquet_physical_type_t physical_type,
                              carquet_encoding_t encoding, const void* expected,
                              size_t value_size, int64_t rows_per_group,
                              int32_t num_row_groups,
                              const int64_t* batch_sizes, size_t num_batches,
                              int64_t max_rows_per_page) {
  int result = 1;
  void* parquet_buffer = NULL;
  size_t parquet_size = 0;
  carquet_error_t error = CARQUET_ERROR_INIT;
  carquet_schema_t* schema = carquet_schema_create(&error);
  carquet_writer_t* writer = NULL;
  carquet_reader_t* reader = NULL;

  if (!schema || carquet_schema_add_column(schema, "value", physical_type, NULL,
                                           CARQUET_REPETITION_REQUIRED, 0,
                                           0) != CARQUET_OK) {
    fprintf(stderr, "%s: schema creation failed: %s\n", name, error.message);
    goto cleanup;
  }

  carquet_writer_options_t options;
  carquet_writer_options_init(&options);
  options.compression = CARQUET_COMPRESSION_UNCOMPRESSED;
  options.dictionary_encoding = CARQUET_ENCODING_PLAIN;
  options.row_group_size = 1024 * 1024;
  options.page_size = 4096;
  options.max_rows_per_page = max_rows_per_page;

  writer = carquet_writer_create_buffer(schema, &options, &error);
  if (!writer ||
      carquet_writer_set_column_encoding(writer, 0, encoding) != CARQUET_OK) {
    fprintf(stderr, "%s: writer creation failed: %s\n", name, error.message);
    goto cleanup;
  }

  for (int32_t group = 0; group < num_row_groups; ++group) {
    int64_t group_offset = 0;
    for (size_t batch = 0; batch < num_batches; ++batch) {
      const int64_t count = batch_sizes[batch];
      const uint8_t* values =
          (const uint8_t*) expected +
          (size_t) (group * rows_per_group + group_offset) * value_size;
      if (carquet_writer_write_batch(writer, 0, values, count, NULL, NULL) !=
          CARQUET_OK) {
        fprintf(stderr, "%s: write_batch failed for group %d batch %zu\n", name,
                group, batch);
        goto cleanup;
      }
      group_offset += count;
    }
    if (group_offset != rows_per_group) {
      fprintf(stderr, "%s: batch sizes do not match the row group\n", name);
      goto cleanup;
    }
    if (group + 1 < num_row_groups &&
        carquet_writer_new_row_group(writer) != CARQUET_OK) {
      fprintf(stderr, "%s: new_row_group failed\n", name);
      goto cleanup;
    }
  }

  if (carquet_writer_close(writer) != CARQUET_OK ||
      carquet_writer_get_buffer(writer, &parquet_buffer, &parquet_size) !=
          CARQUET_OK) {
    fprintf(stderr, "%s: writer finalization failed\n", name);
    writer = NULL;
    goto cleanup;
  }
  writer = NULL;

  reader =
      carquet_reader_open_buffer(parquet_buffer, parquet_size, NULL, &error);
  if (!reader || carquet_reader_num_row_groups(reader) != num_row_groups) {
    fprintf(stderr, "%s: reader creation or row-group count failed: %s\n", name,
            error.message);
    goto cleanup;
  }

  for (int32_t group = 0; group < num_row_groups; ++group) {
    carquet_column_reader_t* column =
        carquet_reader_get_column(reader, group, 0, &error);
    void* actual = calloc((size_t) rows_per_group, value_size);
    if (!column || !actual) {
      fprintf(stderr, "%s: column reader allocation failed\n", name);
      carquet_column_reader_free(column);
      free(actual);
      goto cleanup;
    }
    const int64_t count =
        carquet_column_read_batch(column, actual, rows_per_group, NULL, NULL);
    const uint8_t* group_expected =
        (const uint8_t*) expected +
        (size_t) group * rows_per_group * value_size;
    const int mismatch = count != rows_per_group ||
                         memcmp(actual, group_expected,
                                (size_t) rows_per_group * value_size) != 0;
    carquet_column_reader_free(column);
    free(actual);
    if (mismatch) {
      fprintf(stderr, "%s: value mismatch in row group %d\n", name, group);
      goto cleanup;
    }
  }

  result = 0;

cleanup:
  if (writer) {
    carquet_writer_abort(writer);
  }
  carquet_reader_close(reader);
  carquet_schema_free(schema);
  free(parquet_buffer);
  return result;
}

int main(void) {
  int failures = 0;

  const int64_t boolean_batches[] = {3, 5, 4};
  const uint8_t boolean_values[] = {1, 0, 1, 0, 1, 1, 0, 1, 1, 1, 0, 0};
  failures += validate_roundtrip(
      "BOOLEAN PLAIN 3/5/4", CARQUET_PHYSICAL_BOOLEAN, CARQUET_ENCODING_PLAIN,
      boolean_values, sizeof(boolean_values[0]), 12, 1, boolean_batches,
      sizeof(boolean_batches) / sizeof(boolean_batches[0]), 0);

  const int64_t float_batches[] = {3, 5};
  const float float_values[] = {1.25f,    -2.5f,  3.75f, 100.5f,
                                -200.25f, 0.125f, 42.0f, -7.5f};
  failures += validate_roundtrip(
      "FLOAT BYTE_STREAM_SPLIT 3/5", CARQUET_PHYSICAL_FLOAT,
      CARQUET_ENCODING_BYTE_STREAM_SPLIT, float_values, sizeof(float_values[0]),
      8, 1, float_batches, sizeof(float_batches) / sizeof(float_batches[0]), 0);

  const int64_t boundary_batches[] = {1, 7, 8, 9};
  uint8_t boundary_booleans[50];
  float boundary_floats[50];
  for (size_t i = 0; i < 50; ++i) {
    boundary_booleans[i] = (uint8_t) (((i * 5) + (i / 7)) % 3 == 0);
    boundary_floats[i] = (float) ((int) i * 17 - 193) / 8.0f;
  }
  failures += validate_roundtrip(
      "BOOLEAN PLAIN page/row-group boundaries", CARQUET_PHYSICAL_BOOLEAN,
      CARQUET_ENCODING_PLAIN, boundary_booleans, sizeof(boundary_booleans[0]),
      25, 2, boundary_batches,
      sizeof(boundary_batches) / sizeof(boundary_batches[0]), 8);
  failures += validate_roundtrip(
      "FLOAT BYTE_STREAM_SPLIT page/row-group boundaries",
      CARQUET_PHYSICAL_FLOAT, CARQUET_ENCODING_BYTE_STREAM_SPLIT,
      boundary_floats, sizeof(boundary_floats[0]), 25, 2, boundary_batches,
      sizeof(boundary_batches) / sizeof(boundary_batches[0]), 8);

  if (failures != 0) {
    fprintf(stderr, "%d Carquet multi-batch case(s) failed\n", failures);
    return 1;
  }
  return 0;
}
