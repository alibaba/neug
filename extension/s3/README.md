# S3/OSS Extension for NeuG

## 📖 Overview

The S3 extension provides unified cloud storage access for all NeuG file readers (Parquet, JSON, CSV, etc.). It supports:

- **AWS S3**: Amazon Simple Storage Service
- **Alibaba Cloud OSS**: Object Storage Service with automatic optimization
- **MinIO**: Self-hosted S3-compatible storage
- **Other S3-compatible storage**: Any service implementing S3 API

## 🏗️ Architecture

### Design Principles

1. **Extension-based**: S3 support is implemented as an extension, not in core compiler
2. **Format-agnostic**: Works with all file formats (Parquet, JSON, CSV, etc.)
3. **Transparent integration**: No changes needed in format-specific readers
4. **Unified interface**: Same `FileSystemProvider` interface for all storage backends

### Directory Structure

```
extension/s3/
├── include/
│   └── s3_filesystem.h          # Public API (S3FileSystemProvider, S3URIComponents)
├── src/
│   └── s3_filesystem.cc         # Implementation
└── CMakeLists.txt               # Build configuration
```

### Integration with Other Extensions

```
┌─────────────────────────────────────────┐
│     User Query (Cypher)                 │
│  PARQUET_SCAN('s3://bucket/file')       │
└────────────────┬────────────────────────┘
                 │
                 v
┌─────────────────────────────────────────┐
│  Parquet Extension                      │
│  extension/parquet/                     │
│    - ParquetReadFunction                │
│    - Calls: createFileSystemProvider()  │
└────────────────┬────────────────────────┘
                 │
                 v
┌─────────────────────────────────────────┐
│  S3 Extension                           │
│  extension/s3/                          │
│    - isS3Path() → detect s3://          │
│    - S3FileSystemProvider               │
│    - S3URIComponents::parse()           │
│    - Credential chain                   │
│    - Glob expansion                     │
└────────────────┬────────────────────────┘
                 │
                 v
┌─────────────────────────────────────────┐
│  Arrow S3 FileSystem                    │
│  (Apache Arrow + AWS SDK)               │
│    - HTTP/HTTPS connection              │
│    - Authentication                     │
│    - Object listing                     │
│    - Data streaming                     │
└─────────────────────────────────────────┘
```

## 🔌 Usage by Format Extensions

### Parquet Extension

```cpp
// extension/parquet/include/parquet_read_function.h
#include "../../s3/include/s3_filesystem.h"

static runtime::Context execFunc(
    std::shared_ptr<reader::ReadSharedState> state) {
  // Automatic detection and provider creation
  auto fsProvider = extension::s3::createFileSystemProvider(
      state->schema.file.paths[0]);
  
  auto fileInfo = fsProvider->provide(state->schema.file);
  // ... continue with Parquet reading
}
```

### JSON Extension

```cpp
// extension/json/include/json_read_function.h
#include "../../s3/include/s3_filesystem.h"

static runtime::Context execFunc(
    std::shared_ptr<reader::ReadSharedState> state) {
  // Same API as Parquet!
  auto fsProvider = extension::s3::createFileSystemProvider(
      state->schema.file.paths[0]);
  
  auto fileInfo = fsProvider->provide(state->schema.file);
  // ... continue with JSON reading
}
```

### Future Extensions (CSV, Avro, ORC, etc.)

Any new file format extension can use S3 with the same pattern:

```cpp
#include "../../s3/include/s3_filesystem.h"

auto fsProvider = extension::s3::createFileSystemProvider(path);
auto fileInfo = fsProvider->provide(schema);
// Use fileInfo.fileSystem and fileInfo.resolvedPaths
```

## 🔧 API Reference

### Core Functions

#### `isS3Path(const std::string& path) -> bool`

Detects if a path is an S3/OSS URI.

```cpp
bool is_s3 = extension::s3::isS3Path("s3://bucket/file.parquet");  // true
bool is_local = extension::s3::isS3Path("/local/file.parquet");    // false
```

#### `createFileSystemProvider(const std::string& path)`

Factory function that creates the appropriate provider based on path type.

```cpp
// Returns S3FileSystemProvider for S3 paths
auto provider = extension::s3::createFileSystemProvider("s3://bucket/file");

// Returns LocalFileSystemProvider for local paths  
auto provider = extension::s3::createFileSystemProvider("/local/file");
```

### S3URIComponents

Parses and validates S3 URIs.

```cpp
auto components = extension::s3::S3URIComponents::parse("s3://bucket/path/file.parquet");
// components.bucket = "bucket"
// components.objectKey = "path/file.parquet"
// components.hasGlob = false
```

### S3FileSystemProvider

Main class for S3 access.

```cpp
class S3FileSystemProvider : public function::FileSystemProvider<arrow::fs::FileSystem> {
 public:
  FileInfo<arrow::fs::FileSystem> provide(const reader::FileSchema& schema) override;
};
```

**Features:**
- Credential chain (explicit → env → file → IAM/RAM)
- OSS/MinIO auto-detection and optimization
- Glob pattern expansion
- Internal/VPC endpoint support

## 📝 Configuration Options

All options are passed via `FileSchema.options`:

| Option | Description | Example |
|--------|-------------|---------|
| `ENDPOINT_OVERRIDE` | Custom S3 endpoint | `https://oss-cn-hangzhou.aliyuncs.com` |
| `AWS_REGION` | Region name | `oss-cn-hangzhou` |
| `ACCESS_KEY_ID` | Access key | `LTAI5t...` |
| `SECRET_ACCESS_KEY` | Secret key | `your_secret` |
| `VIRTUAL_HOSTED_STYLE` | Use virtual-hosted URLs | `false` |

## 🌍 OSS Support

The extension has special optimizations for Alibaba Cloud OSS:

### Auto-Detection

```cpp
// Automatically detects OSS from endpoint
if (endpoint.find("aliyuncs.com") != std::string::npos) {
  // Use OSS-specific optimizations:
  // - Default region: oss-cn-hangzhou
  // - Path-style URLs
  // - Appropriate scheme (HTTP/HTTPS)
}
```

### Usage Example

```cypher
-- Read from OSS
MATCH (n)
RETURN PARQUET_SCAN('s3://my-bucket/data.parquet', {
  ENDPOINT_OVERRIDE: 'https://oss-cn-hangzhou.aliyuncs.com',
  ACCESS_KEY_ID: 'LTAI5t...',
  SECRET_ACCESS_KEY: 'your_secret'
})

-- Also works with JSON!
MATCH (n)
RETURN JSON_SCAN('s3://my-bucket/data.json', {
  ENDPOINT_OVERRIDE: 'https://oss-cn-hangzhou.aliyuncs.com'
})
```

## 🔐 Security

### Credential Chain Priority

1. **Explicit credentials** from `FileSchema.options`
2. **Environment variables**: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
3. **Credentials file**: `~/.aws/credentials`
4. **IAM/RAM roles**: EC2/ECS instance metadata

### Best Practices

1. **Use IAM/RAM roles** when running on cloud instances
2. **Environment variables** for local development
3. **Avoid hardcoding** credentials in queries

## 🚀 Performance

### Optimizations

1. **Arrow zero-copy**: Data flows directly from S3 to Arrow arrays
2. **Glob expansion**: Server-side filtering reduces network traffic
3. **Internal endpoints**: Use OSS internal endpoints for ECS instances
4. **Connection pooling**: Reuses HTTP connections (handled by AWS SDK)

### Monitoring

The extension logs all operations at INFO level:

```
Detected OSS endpoint, using default region: oss-cn-hangzhou
Using custom endpoint: https://oss-cn-hangzhou.aliyuncs.com
Using path-style URLs for OSS
S3FileSystem initialized successfully, resolved 10 paths
Glob pattern matched 10 files
```

## 🧪 Testing

### Unit Tests

```bash
# Run S3 URI parser tests
./build/tests/unittest/s3_uri_parser_test
```

### Integration Tests

```bash
# Set OSS credentials
export OSS_ENDPOINT=oss-cn-hangzhou.aliyuncs.com
export OSS_ACCESS_KEY_ID=your_key
export OSS_SECRET_ACCESS_KEY=your_secret
export OSS_TEST_BUCKET=test-bucket
export OSS_INTEGRATION_TEST=1

# Run integration tests
./build/tests/e2e/oss_integration_test
```

## 🔄 Migration from Old Architecture

### Before (Compiler-based)

```
src/compiler/function/read_function.cc  ← S3 code in compiler
include/neug/compiler/function/read_function.h
```

**Issues:**
- S3 code mixed with core compiler
- Hard to reuse across format readers
- Tight coupling

### After (Extension-based)

```
extension/s3/src/s3_filesystem.cc       ← S3 code in extension
extension/s3/include/s3_filesystem.h
extension/parquet/                      ← Uses s3 extension
extension/json/                         ← Uses s3 extension
```

**Benefits:**
- Clean separation of concerns
- Reusable across all format readers
- Easy to maintain and extend
- Format readers remain simple

## 📦 Building

The S3 extension is built automatically with NeuG:

```bash
cmake -DBUILD_EXTENSIONS=parquet,json -Bbuild
cmake --build build
```

**Note**: Arrow S3 support must be enabled in `cmake/BuildArrowAsThirdParty.cmake`:

```cmake
set(ARROW_S3 ON CACHE BOOL "" FORCE)
```

## 🤝 Contributing

To add S3 support to a new format extension:

1. Add dependency in `extension/YOUR_FORMAT/CMakeLists.txt`:
   ```cmake
   target_link_libraries(neug_YOUR_FORMAT_extension PRIVATE neug_s3_extension)
   target_include_directories(neug_YOUR_FORMAT_extension PRIVATE 
       ${CMAKE_SOURCE_DIR}/extension/s3/include)
   ```

2. Include the header in your reader function:
   ```cpp
   #include "../../s3/include/s3_filesystem.h"
   ```

3. Use the factory function:
   ```cpp
   auto fsProvider = extension::s3::createFileSystemProvider(path);
   auto fileInfo = fsProvider->provide(schema);
   ```

That's it! Your format now supports S3/OSS/MinIO automatically.

## 📚 See Also

- [OSS Quick Start Guide](../../specs/005-s3-read-extension/OSS_QUICKSTART.md)
- [Apache Arrow S3 Documentation](https://arrow.apache.org/docs/cpp/api/filesystems.html)
- [AWS SDK Documentation](https://aws.amazon.com/sdk-for-cpp/)
- [Alibaba Cloud OSS Documentation](https://help.aliyun.com/product/31815.html)
