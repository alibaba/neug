{
  global:
    /* Export everything by default */
    *;

  local:
    /* Hide all Apache Arrow C++ symbols to avoid conflicts with pyarrow.
     * Mangled C++ names in the arrow:: namespace start with _ZN5arrow. */
    _ZN5arrow*;
    _ZNK5arrow*;
    _ZTI*5arrow*;
    _ZTV*5arrow*;
    _ZTS*5arrow*;
    _ZTh*5arrow*;

    /* Parquet symbols */
    _ZN7parquet*;
    _ZNK7parquet*;
    _ZTI*7parquet*;
    _ZTV*7parquet*;
    _ZTS*7parquet*;

    /* Protobuf symbols (google::protobuf::*) */
    _ZN6google8protobuf*;
    _ZNK6google8protobuf*;
    _ZTI*8protobuf*;
    _ZTV*8protobuf*;
    _ZTS*8protobuf*;
    _ZGV*8protobuf*;
    _TableStruct_google*;
    /* pb:: namespace (protobuf internal) */
    _ZN2pb*;
    _ZNK2pb*;

    /* Abseil symbols (absl::*) */
    _ZN4absl*;
    _ZNK4absl*;
    _ZTI*4absl*;
    _ZTV*4absl*;
    _ZTS*4absl*;

    /* utf8_range (protobuf dependency) */
    utf8_range*;
};
