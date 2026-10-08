/** Copyright 2020 Alibaba Group Holding Limited.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 */
#pragma once

#include <carquet/carquet.h>

#include <cstdint>
#include <cstdlib>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace neug::parquet::test {

inline void check(carquet_status_t status) {
  if (status != CARQUET_OK) {
    throw std::runtime_error(carquet_status_string(status));
  }
}

using BufferWriter =
    std::unique_ptr<carquet_writer_t, decltype(&carquet_writer_abort)>;

inline BufferWriter writerFor(const carquet_schema_t* schema,
                              bool writeSchema = true) {
  carquet_writer_options_t options;
  carquet_writer_options_init(&options);
  options.compression = CARQUET_COMPRESSION_ZSTD;
  options.write_arrow_schema = writeSchema;
  carquet_error_t error = CARQUET_ERROR_INIT;
  BufferWriter writer(carquet_writer_create_buffer(schema, &options, &error),
                      carquet_writer_abort);
  if (!writer) {
    throw std::runtime_error(error.message);
  }
  return writer;
}

inline std::shared_ptr<std::vector<uint8_t>> finish(BufferWriter writer) {
  check(carquet_writer_close(writer.get()));
  void* data = nullptr;
  size_t size = 0;
  check(carquet_writer_get_buffer(writer.get(), &data, &size));
  writer.release();  // get_buffer consumes the closed buffer writer.
  std::unique_ptr<void, decltype(&std::free)> buffer(data, std::free);
  return std::make_shared<std::vector<uint8_t>>(
      static_cast<uint8_t*>(data), static_cast<uint8_t*>(data) + size);
}

// Two row groups with independent expected values for filtering and projection.
inline std::shared_ptr<std::vector<uint8_t>> scanFile(bool idOnly = false) {
  carquet_error_t error = CARQUET_ERROR_INIT;
  std::unique_ptr<carquet_schema_t, decltype(&carquet_schema_free)> schema(
      carquet_schema_create(&error), carquet_schema_free);
  if (!schema) {
    throw std::runtime_error(error.message);
  }
  check(carquet_schema_add_column(schema.get(), "id", CARQUET_PHYSICAL_INT64,
                                  nullptr, CARQUET_REPETITION_REQUIRED, 0, 0));
  if (!idOnly) {
    for (const auto* name : {"enabled", "signed_value", "unsigned_value"}) {
      check(carquet_schema_add_column(schema.get(), name,
                                      CARQUET_PHYSICAL_INT32, nullptr,
                                      CARQUET_REPETITION_REQUIRED, 0, 0));
    }
    check(carquet_schema_add_column(schema.get(), "score",
                                    CARQUET_PHYSICAL_DOUBLE, nullptr,
                                    CARQUET_REPETITION_OPTIONAL, 0, 0));
    carquet_logical_type_t stringType{};
    stringType.id = CARQUET_LOGICAL_STRING;
    check(carquet_schema_add_column(schema.get(), "label",
                                    CARQUET_PHYSICAL_BYTE_ARRAY, &stringType,
                                    CARQUET_REPETITION_OPTIONAL, 0, 0));
  }
  auto writer = writerFor(schema.get());
  for (int64_t group = 0; group < 2; ++group) {
    const int64_t ids[] = {group * 2 + 1, group * 2 + 2};
    check(
        carquet_writer_write_batch(writer.get(), 0, ids, 2, nullptr, nullptr));
    if (!idOnly) {
      const int32_t values[] = {1, 0};
      for (int column = 1; column < 4; ++column) {
        check(carquet_writer_write_batch(writer.get(), column, values, 2,
                                         nullptr, nullptr));
      }
      const int16_t defined[] = {static_cast<int16_t>(group == 0), 1};
      const double scores[] = {group == 0 ? 1.25 : 4.5, -2.5};
      check(carquet_writer_write_batch(writer.get(), 4, scores, 2, defined,
                                       nullptr));
      std::string labels[] = {group == 0 ? "" : "中文", "hello"};
      carquet_byte_array_t strings[2];
      for (size_t i = 0; i < 2; ++i) {
        strings[i] = {reinterpret_cast<uint8_t*>(labels[i].data()),
                      static_cast<int32_t>(labels[i].size())};
      }
      check(carquet_writer_write_batch(writer.get(), 5, strings, 2, defined,
                                       nullptr));
    }
    if (group == 0) {
      check(carquet_writer_new_row_group(writer.get()));
    }
  }
  return finish(std::move(writer));
}

// C Data is an interchange ABI; this fixture needs no Arrow library or PyArrow.
inline std::shared_ptr<std::vector<uint8_t>> nestedFile() {
  ArrowSchema item{
      .format = "l", .name = "element", .flags = ARROW_FLAG_NULLABLE};
  ArrowSchema fixedItem{
      .format = "g", .name = "element", .flags = ARROW_FLAG_NULLABLE};
  ArrowSchema matrixItem{
      .format = "i", .name = "element", .flags = ARROW_FLAG_NULLABLE};
  ArrowSchema* itemChildren[] = {&item};
  ArrowSchema* fixedChildren[] = {&fixedItem};
  ArrowSchema* matrixChildren[] = {&matrixItem};
  ArrowSchema items{.format = "+l",
                    .name = "items",
                    .flags = ARROW_FLAG_NULLABLE,
                    .n_children = 1,
                    .children = itemChildren};
  ArrowSchema fixed{.format = "+l",
                    .name = "fixed3",
                    .n_children = 1,
                    .children = fixedChildren};
  ArrowSchema inner{.format = "+l",
                    .name = "element",
                    .n_children = 1,
                    .children = matrixChildren};
  ArrowSchema* innerChildren[] = {&inner};
  ArrowSchema matrix{.format = "+l",
                     .name = "matrix2x2",
                     .n_children = 1,
                     .children = innerChildren};
  ArrowSchema* fields[] = {&items, &fixed, &matrix};
  ArrowSchema root{.format = "+s", .n_children = 3, .children = fields};
  carquet_error_t error = CARQUET_ERROR_INIT;
  carquet_schema_t* raw = nullptr;
  check(carquet_arrow_import_schema(&root, &raw, &error));
  std::unique_ptr<carquet_schema_t, decltype(&carquet_schema_free)> schema(
      raw, carquet_schema_free);
  auto writer = writerFor(schema.get(), false);
  // Fixed-size lists use the same Parquet LIST layout. Supply independently
  // serialized IPC schema metadata to exercise the reader's refinement parser.
  // Generated with base64.b64encode(pa.schema([
  //   pa.field("items", pa.list_(pa.int64())),
  //   pa.field("fixed3", pa.list_(pa.float64(), 3), nullable=False),
  //   pa.field("matrix2x2", pa.list_(pa.list_(pa.int32(), 2), 2),
  //   nullable=False)
  // ]).serialize().to_pybytes()).decode(). This is schema-only test data.
  constexpr const char* arrowSchema =
      "/////8ABAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAMA"
      "AAAYAQAArAAAAAQAAABs////AAAAEBQAAAAgAAAABAAAAAEAAAAcAAAACQAAAG1hdHJpeDJ4"
      "MgAAAF7///8CAAAA+P7//wAAARAUAAAAHAAAAAQAAAABAAAAGAAAAAQAAABpdGVtAAAAAI7/"
      "//8CAAAAKP///wAAAQIQAAAAGAAAAAQAAAAAAAAABAAAAGl0ZW0AAAAAGP///wAAAAEgAAAA"
      "EAAUAAgAAAAHAAwAAAAQABAAAAAAAAAQFAAAACQAAAAEAAAAAQAAACAAAAAGAAAAZml4ZWQz"
      "AAAAAAYACAAEAAYAAAADAAAAoP///wAAAQMQAAAAHAAAAAQAAAAAAAAABAAAAGl0ZW0AAAYA"
      "CAAGAAYAAAAAAAIA0P///wAAAQwUAAAAIAAAAAQAAAABAAAAKAAAAAUAAABpdGVtcwAAAAQA"
      "BAAEAAAAEAAUAAgABgAHAAwAAAAQABAAAAAAAAECEAAAACAAAAAEAAAAAAAAAAQAAABpdGVt"
      "AAAAAAgADAAIAAcACAAAAAAAAAFAAAAA";
  check(carquet_writer_add_metadata(writer.get(), "ARROW:schema", arrowSchema));
  const int64_t values[] = {1, 0, 3, 4, 5};
  const double fixedValues[] = {1, 0, 3, 4, 5, 6, 0, 0, 0, -1, -2, -3};
  const int32_t matrixValues[] = {1, 2, 3,  4,  5,  6,  7,  8,
                                  9, 0, 11, 12, -1, -2, -3, -4};
  const uint8_t validItems = 0x1d, validLists = 0x0b;
  const uint8_t validFixed[] = {0xfd, 0x0f}, validMatrix[] = {0xff, 0xfd};
  const int32_t offsets[] = {0, 3, 3, 3, 5};
  const void* itemBuffers[] = {&validItems, values};
  const void* listBuffers[] = {&validLists, offsets};
  const void* fixedBuffers[] = {validFixed, fixedValues};
  const void* matrixBuffers[] = {validMatrix, matrixValues};
  const void* containerBuffers[] = {nullptr};
  const int32_t fixedOffsets[] = {0, 3, 6, 9, 12};
  const int32_t innerOffsets[] = {0, 2, 4, 6, 8, 10, 12, 14, 16};
  const int32_t outerOffsets[] = {0, 2, 4, 6, 8};
  const void* fixedListBuffers[] = {nullptr, fixedOffsets};
  const void* innerListBuffers[] = {nullptr, innerOffsets};
  const void* outerListBuffers[] = {nullptr, outerOffsets};
  ArrowArray itemArray{
      .length = 5, .null_count = 1, .n_buffers = 2, .buffers = itemBuffers};
  ArrowArray fixedArray{
      .length = 12, .null_count = 1, .n_buffers = 2, .buffers = fixedBuffers};
  ArrowArray matrixArray{
      .length = 16, .null_count = 1, .n_buffers = 2, .buffers = matrixBuffers};
  ArrowArray* itemArrays[] = {&itemArray};
  ArrowArray* fixedArrays[] = {&fixedArray};
  ArrowArray* matrixArrays[] = {&matrixArray};
  ArrowArray listArray{.length = 4,
                       .null_count = 1,
                       .n_buffers = 2,
                       .n_children = 1,
                       .buffers = listBuffers,
                       .children = itemArrays};
  ArrowArray fixedList{.length = 4,
                       .n_buffers = 2,
                       .n_children = 1,
                       .buffers = fixedListBuffers,
                       .children = fixedArrays};
  ArrowArray innerArray{.length = 8,
                        .n_buffers = 2,
                        .n_children = 1,
                        .buffers = innerListBuffers,
                        .children = matrixArrays};
  ArrowArray* innerArrays[] = {&innerArray};
  ArrowArray matrixList{.length = 4,
                        .n_buffers = 2,
                        .n_children = 1,
                        .buffers = outerListBuffers,
                        .children = innerArrays};
  ArrowArray* columns[] = {&listArray, &fixedList, &matrixList};
  ArrowArray array{.length = 4,
                   .n_buffers = 1,
                   .n_children = 3,
                   .buffers = containerBuffers,
                   .children = columns};
  check(carquet_writer_write_arrow(writer.get(), &array, &root, &error));
  return finish(std::move(writer));
}

}  // namespace neug::parquet::test
