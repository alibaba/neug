# S3 Extension

The S3 Extension enables `LOAD FROM` to read files stored on S3-compatible object storage services (AWS S3, Alibaba Cloud OSS, MinIO, etc.) and over HTTP/HTTPS URLs. After loading the S3 Extension, NeuG can resolve `s3://`, `oss://`, and `http://`/`https://` paths transparently in all `LOAD FROM` queries.

## Install Extension

```cypher
INSTALL S3;
```

## Load Extension

```cypher
LOAD S3;
```

## Supported URI Schemes

| Scheme                 | Protocol                            | Example                                |
| ---------------------- | ----------------------------------- | -------------------------------------- |
| `s3://`                | AWS S3 or any S3-compatible service | `s3://my-bucket/path/to/file.parquet`  |
| `oss://`               | Alibaba Cloud OSS                   | `oss://my-bucket/path/to/file.parquet` |
| `http://` / `https://` | HTTP/HTTPS direct URL               | `http://example.com/data/file.parquet` |

## Configuration Options

Inline options are passed inside parentheses after the file path in a `LOAD FROM` query. All option names are case-insensitive.

### Credential Options

| Option                                            | Type   | Default   | Description                                                                                             |
| ------------------------------------------------- | ------ | --------- | ------------------------------------------------------------------------------------------------------- |
| `CREDENTIALS_KIND`                                | string | `Default` | Credential mode:`Default`, `Anonymous`, or `Explicit`. See [Credential Modes](#credential-modes) below. |
| `OSS_ACCESS_KEY_ID` / `AWS_ACCESS_KEY_ID`         | string | —        | Access key ID. Required when`CREDENTIALS_KIND='Explicit'`.                                              |
| `OSS_ACCESS_KEY_SECRET` / `AWS_SECRET_ACCESS_KEY` | string | —        | Secret access key. Required when`CREDENTIALS_KIND='Explicit'`.                                          |

### Endpoint and Region Options

| Option                                                    | Type   | Default       | Description                                                                                                                          |
| --------------------------------------------------------- | ------ | ------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `OSS_ENDPOINT` / `AWS_ENDPOINT_URL` / `ENDPOINT_OVERRIDE` | string | —            | Custom endpoint URL for S3-compatible services (OSS, MinIO, etc.).                                                                   |
| `OSS_REGION` / `AWS_DEFAULT_REGION`                       | string | auto-detected | AWS/OSS region (e.g.,`us-east-1`, `oss-cn-beijing`). For OSS endpoints, the region can also be auto-extracted from the endpoint URL. |

### Timeout Options

| Option            | Type   | Default | Description                    |
| ----------------- | ------ | ------- | ------------------------------ |
| `CONNECT_TIMEOUT` | double | `5.0`   | Connection timeout in seconds. |
| `REQUEST_TIMEOUT` | double | `30.0`  | Request timeout in seconds.    |

## Credential Modes

### `Default` (default)

Arrow SDK's default credential provider chain is used in order:

1. Environment variables `AWS_ACCESS_KEY_ID` + `AWS_SECRET_ACCESS_KEY`
2. `~/.aws/credentials` and `~/.aws/config` files
3. EC2 instance metadata (IAM role)
4. ECS task role

Use this mode when credentials are provided through the execution environment.

### `Anonymous`

No credentials are sent. Use this mode for publicly accessible buckets.

### `Explicit`

Access key and secret key are specified directly in the query options.

## Query Examples

### Load from AWS S3 (Default credentials)

Uses the credential provider chain configured in the environment:

```cypher
LOAD FROM "s3://my-bucket/data/person.parquet"
RETURN *;
```

### Load from Alibaba Cloud OSS (Anonymous, public bucket)

```cypher
LOAD FROM "oss://my-bucket/data/person.parquet" (
    CREDENTIALS_KIND='Anonymous',
    ENDPOINT_OVERRIDE='oss-cn-beijing.aliyuncs.com'
)
RETURN *;
```

### Load from OSS with Explicit Credentials

```cypher
LOAD FROM "oss://my-bucket/data/person.parquet" (
    CREDENTIALS_KIND='Explicit',
    ENDPOINT_OVERRIDE='oss-cn-beijing.aliyuncs.com',
    OSS_ACCESS_KEY_ID='your-access-key-id',
    OSS_ACCESS_KEY_SECRET='your-access-key-secret'
)
RETURN *;
```

### Load from HTTP URL

```cypher
LOAD FROM "http://example.com/data/person.parquet"
RETURN *;
```
### Glob Pattern

Load multiple files matching a pattern:

```cypher
LOAD FROM "s3://my-bucket/data/*.parquet"
RETURN *;
```

## Combining with Other Extensions

The S3 Extension provides the virtual filesystem (VFS) layer only. To load Parquet files from S3/OSS/HTTP, both extensions must be loaded:

```cypher
LOAD S3;
LOAD PARQUET;

LOAD FROM "oss://my-bucket/data/person.parquet" (
    CREDENTIALS_KIND='Anonymous',
    ENDPOINT_OVERRIDE='oss-cn-beijing.aliyuncs.com'
)
RETURN *;
```

> **Note:** All relational operations supported by `LOAD FROM` — including type conversion, WHERE filtering, aggregation, sorting, and limiting — work the same way with remote files. See the [LOAD FROM reference](../data_io/load_data) for the complete list of operations.

