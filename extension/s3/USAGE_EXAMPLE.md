# S3 Extension Usage Examples

## Build Configuration

### Enable S3 Support

S3 support is controlled by the `BUILD_EXTENSIONS` CMake option. To enable S3:

```bash
# Method 1: Build with S3 extension (recommended)
cd neug
mkdir build && cd build
cmake -DBUILD_EXTENSIONS="json;parquet;s3" ..
make -j$(nproc)

# Method 2: Build with all common extensions
cmake -DBUILD_EXTENSIONS="json;parquet;s3" ..
make -j$(nproc)
```

When S3 is included in `BUILD_EXTENSIONS`:
- `ARROW_S3=ON` is automatically set
- AWS SDK libraries are compiled (~5-10 min additional build time)
- S3/OSS filesystem support is available in CSV/JSON/Parquet readers

### Build WITHOUT S3 Support

```bash
# Only local file support
cmake -DBUILD_EXTENSIONS="json;parquet" ..
make -j$(nproc)
```

This configuration:
- `ARROW_S3=OFF` (default when s3 not in BUILD_EXTENSIONS)
- Faster build (~7 min saved)
- Smaller binaries (~45 MB saved)
- Only local filesystem access available

---

## Runtime Usage Examples

### 1. Load JSON from OSS (Alibaba Cloud)

```cypher
-- Load the extensions
LOAD json;
LOAD s3;

-- Read from OSS with explicit credentials
LOAD FROM "oss://my-bucket/data/users.json" (
    CREDENTIALS_KIND='Static',
    ACCESS_KEY='YOUR_ACCESS_KEY',
    SECRET_KEY='YOUR_SECRET_KEY',
    ENDPOINT_OVERRIDE='oss-cn-hangzhou.aliyuncs.com',
    REGION='oss-cn-hangzhou'
) RETURN *;
```

### 2. Load Parquet from AWS S3

```cypher
LOAD parquet;
LOAD s3;

-- Using IAM role (no credentials needed)
LOAD FROM "s3://my-bucket/data/events.parquet" (
    CREDENTIALS_KIND='Default',
    REGION='us-west-2'
) RETURN *;

-- With explicit credentials
LOAD FROM "s3://my-bucket/data/events.parquet" (
    CREDENTIALS_KIND='Static',
    ACCESS_KEY='YOUR_AWS_ACCESS_KEY',
    SECRET_KEY='YOUR_AWS_SECRET_KEY',
    REGION='us-west-2'
) RETURN *;
```

### 3. Load CSV from S3 (MinIO compatible)

```cypher
LOAD csv;
LOAD s3;

LOAD FROM "s3://my-bucket/data/products.csv" (
    CREDENTIALS_KIND='Static',
    ACCESS_KEY='minioadmin',
    SECRET_KEY='minioadmin',
    ENDPOINT_OVERRIDE='localhost:9000',
    REGION='us-east-1',
    SCHEME='http'  -- MinIO often uses HTTP locally
) RETURN *;
```

### 4. Load from Local Files (no S3 needed)

```cypher
-- Works regardless of whether S3 is compiled in
LOAD json;
LOAD FROM "/local/path/data.json" RETURN *;

LOAD parquet;
LOAD FROM "/local/path/data.parquet" RETURN *;

LOAD csv;
LOAD FROM "/local/path/data.csv" RETURN *;
```

---

## Python API Examples

### With S3 Support

```python
import neug

# Connect to database
conn = neug.connect()

# Load extensions
conn.execute("LOAD json")
conn.execute("LOAD s3")

# Read from OSS
result = conn.execute("""
    LOAD FROM "oss://my-bucket/data.json" (
        CREDENTIALS_KIND='Explicit',
        OSS_ACCESS_KEY_ID='YOUR_KEY',
        OSS_ACCESS_KEY_SECRET='YOUR_SECRET',
        OSS_ENDPOINT='oss-cn-beijing.aliyuncs.com',
        OSS_REGION='oss-cn-beijing'
    ) RETURN *
""")

for row in result:
    print(row)
```

### Local Files Only

```python
import neug

conn = neug.connect()
conn.execute("LOAD json")

# No need to load s3 for local files
result = conn.execute('LOAD FROM "/local/data.json" RETURN *')
for row in result:
    print(row)
```

---

## Testing S3 Integration

### C++ Tests

```bash
# Build with S3 extension
cd build
cmake -DBUILD_EXTENSIONS="json;parquet;s3" -DBUILD_TEST=ON ..
make -j$(nproc)

# Run S3 tests (requires OSS credentials in environment)
export OSS_ACCESS_KEY_ID="your_key"
export OSS_ACCESS_KEY_SECRET="your_secret"
export OSS_ENDPOINT="oss-cn-hangzhou.aliyuncs.com"
export OSS_REGION="oss-cn-hangzhou"

./tests/extension/s3_extension_test
```

### Python Tests

```bash
# Run end-to-end tests
cd tools/python_bind
pytest tests/ -v -k "oss or s3"
```

---

## Environment Variables (Optional)

For convenience, you can set default OSS/S3 credentials via environment variables:

```bash
# OSS credentials
export OSS_ACCESS_KEY_ID="your_oss_key"
export OSS_ACCESS_KEY_SECRET="your_oss_secret"
export OSS_ENDPOINT="oss-cn-hangzhou.aliyuncs.com"
export OSS_REGION="oss-cn-hangzhou"

# AWS S3 credentials
export AWS_ACCESS_KEY_ID="your_aws_key"
export AWS_SECRET_ACCESS_KEY="your_aws_secret"
export AWS_REGION="us-west-2"
```

Then you can use simplified syntax:

```cypher
-- OSS with env vars
LOAD FROM "oss://my-bucket/data.json" (
    CREDENTIALS_KIND='Anonymous'  -- Will use env vars
) RETURN *;
```

---

## Troubleshooting

### Error: "S3/OSS path given but S3 support is not compiled in"

**Solution**: Rebuild with S3 extension:
```bash
cmake -DBUILD_EXTENSIONS="json;parquet;s3" ..
make clean && make -j$(nproc)
```

### Credential Errors

Verify your credentials:
```bash
# For OSS
curl -I "https://your-bucket.oss-cn-hangzhou.aliyuncs.com"

# For S3
aws s3 ls s3://your-bucket/ --region us-west-2
```

### Region/Endpoint Mismatch

Ensure `REGION` matches `ENDPOINT_OVERRIDE`:
- OSS Beijing: `REGION='oss-cn-beijing'`, `ENDPOINT_OVERRIDE='oss-cn-beijing.aliyuncs.com'`
- OSS Hangzhou: `REGION='oss-cn-hangzhou'`, `ENDPOINT_OVERRIDE='oss-cn-hangzhou.aliyuncs.com'`

---

## Architecture Notes

The current implementation uses a **static filesystem provider factory**:

- **Compile-time selection**: S3 support is determined by `ARROW_S3` flag during build
- **Format-agnostic**: CSV/JSON/Parquet readers use the same factory (`CreateFileSystemProviderFromUri`)
- **Protocol detection**: Factory automatically selects provider based on URI scheme:
  - `file://` or no scheme → `LocalFileSystemProvider`
  - `s3://` → `S3FileSystemProvider` (if ARROW_S3=ON)
  - `oss://` → `S3FileSystemProvider` (treated as S3-compatible, if ARROW_S3=ON)
