/* Linker version-script for libneug.so and other neug binaries.
 *
 * Strategy: export everything by default, then selectively hide third-party
 * symbols that would conflict with pyarrow's bundled copies.
 *
 * NOTE: we intentionally do NOT use "local: *" (hide-everything-by-default)
 * because GNU ld applies local binding to UNDEF entries too.  Arrow's static
 * sub-libraries (dataset, acero, csv, io, parquet) contain cross-references
 * that remain as UNDEF in the .so; marking them local causes downstream
 * executables to fail with "undefined reference" errors.
 */
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
