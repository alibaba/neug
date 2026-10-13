# Carquet compatibility fixtures

These Parquet files are immutable interoperability fixtures. They were produced
offline with Apache Arrow PyArrow 23.0.1 and are read by the tests without
loading or linking Arrow. The expected NeuG schema and values are recorded in
`carquet_golden_compatibility_test.cc`; changing a fixture requires updating
both that contract and this manifest after checking the physical format and
logical values with Apache Arrow.

## Recorded contracts

- `arrow_types.parquet` contains four rows and two row groups. It covers Boolean,
  signed and unsigned 8/16/32/64-bit integers, float and double special values,
  UTF-8 and large UTF-8 strings, date32, timestamp s/ms/us/ns, a timestamp with
  `Asia/Shanghai` metadata, list, large-list, and fixed-size-list values. It uses
  Snappy, dictionary encoding, page indexes, page checksums, and embedded Arrow
  schema metadata. The recorded NeuG behavior widens 8/16-bit integers to
  32-bit values, truncates timestamps to milliseconds, and represents the
  timezone-bearing timestamp as NeuG's timezone-free timestamp.
- `arrow_delta_encodings.parquet` contains 67 rows in four row groups. Its
  columns use DELTA_BINARY_PACKED with Snappy/Gzip,
  DELTA_LENGTH_BYTE_ARRAY with Zstd, and DELTA_BYTE_ARRAY with LZ4. It uses data
  page V2, page checksums, no statistics, and no embedded Arrow schema. For row
  `r` in `[0, 66]`, the recorded values are `r - 17`, `r * 37 - 911`,
  `length-{r}-` followed by `r % 7` copies of `x`, and
  `shared-prefix-{r / 4}-value-{r}`.
- `arrow_byte_stream_split.parquet` contains 12 rows in three row groups. Its
  float and double columns use BYTE_STREAM_SPLIT, LZ4, data page V2, page
  indexes, and page checksums. The golden arrays, including NaN, infinities, and
  signed zero, are recorded directly in the C++ test.
- `arrow_int96.parquet` contains six rows in three row groups. Its timestamp is
  physically encoded as deprecated INT96 and covers nanosecond values before,
  at, and after the Unix epoch, a modern timestamp, and null. The recorded
  Arrow behavior exposes `timestamp[ns]`; NeuG truncates it toward zero to
  millisecond precision.
- `arrow_empty.parquet` records the Arrow behavior for a schema-bearing file
  with zero rows.
- `arrow_all_null_groups.parquet` contains six rows in three row groups; its
  string and list columns are entirely null.

The legacy NeuG Arrow reader cannot materialize Arrow INT8/INT16/UINT8/UINT16
arrays even though its schema converter widens those types. Their golden values
therefore come from Arrow's logical values plus NeuG's documented widening
contract. Contracts supported by the legacy NeuG reader were also checked
through that reader. Negative timestamps use exact whole milliseconds here
because NeuG's shared Python result conversion currently rejects negative
millisecond remainders; the C++ Carquet conversion has separate coverage for
that case.

## Checksums

```text
0cee75ea8fa2bb1c1bbb3154f8153568d4b78f8bb0b0d29e5a07e641ec25df5c  arrow_all_null_groups.parquet
20c666ffd6c57ab5dbde8ae93b4dd88376eddecab6415ab2d13bbc056a26fa5a  arrow_byte_stream_split.parquet
7337f1a3ff555e19a3f97cdaf2990d5d3cf1c68c7acd35f347507e4963167c70  arrow_delta_encodings.parquet
656ee03ae30dd57dea40b623bafc823499ac240260f42460a2e2c6ac85d035b4  arrow_empty.parquet
671e49303eaa7acc5702cf71ab817a29967400abe060b2f45fe2bc328bf111d3  arrow_int96.parquet
6bfaa4ce5c2b42d95a51a93172384d22fe6475f2bc0e93796e8f52a930bb0dd3  arrow_types.parquet
```
