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

    /* AWS C++ SDK (bundled by Arrow when ARROW_S3=ON). Already mostly
     * suppressed by --exclude-libs,libarrow_bundled_dependencies.a in
     * neug_symbol_visibility.cmake, but listed here for symmetry with the
     * macOS unexported list and to guard against future link-line changes
     * that might re-expose them. */
    _ZN3Aws*;
    _ZNK3Aws*;
    _ZTI*3Aws*;
    _ZTV*3Aws*;
    _ZTS*3Aws*;
    _ZTT*3Aws*;
    _ZTC*3Aws*;
    _ZTh*3Aws*;
    _ZTv*3Aws*;

    /* AWS C SDK (aws-c-common, aws-c-cal, aws-c-io, aws-c-http, aws-c-auth,
     * aws-c-mqtt, aws-c-s3, aws-c-sdkutils, aws-checksums, aws-crt-cpp). */
    aws_*;
    s_aws_*;
    g_aws_*;
};
