/* Linker version-script for libneug.so and other neug binaries.
 *
 * Strategy: export everything by default, then selectively hide third-party
 * symbols that would conflict with pyarrow's bundled copies.
 */
{
  global:
    /* Export everything by default */
    *;

  local:
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
