# HTTPFS Extension

The HTTPFS Extension enables NeuG to access files stored on S3-compatible object storage services (AWS S3, Alibaba Cloud OSS, MinIO, etc.) and over HTTP/HTTPS URLs. After loading the HTTPFS Extension, NeuG can resolve `s3://`, `oss://`, and `http://`/`https://` paths transparently in both `LOAD FROM` (read) and `COPY TO` (write) queries.

## Install Extension

```cypher
INSTALL HTTPFS;
```

## Load Extension

```cypher
LOAD HTTPFS;
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

### TLS and Addressing Options

| Option         | Type   | Default | Description                                                                                                                                  |
| -------------- | ------ | ------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `VERIFY_SSL`   | bool   | `true`  | Verify the server's TLS certificate. Set to `false` only for trusted test environments (e.g. self-signed MinIO).                             |
| `CA_CERT_FILE` | string | auto    | Path to a CA bundle used for TLS verification. When unset, the system default locations are probed (also honors the `SSL_CERT_FILE` env var). |
| `PATH_STYLE`   | bool   | `false` | Use path-style addressing (`endpoint/bucket/key`) instead of virtual hosted style (`bucket.endpoint/key`). Automatically enabled when the endpoint is an IP address or `localhost` (typical for MinIO). |

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

S3/OSS requests are signed with AWS Signature Version 4. Credentials are
resolved from query options and environment variables only — **STS tokens,
`~/.aws/credentials` files, and IAM/ECS instance roles are not supported**.

### `Default` (default)

Credentials are looked up in this order:

1. Query options: `OSS_ACCESS_KEY_ID` / `AWS_ACCESS_KEY_ID` and
   `OSS_ACCESS_KEY_SECRET` / `AWS_SECRET_ACCESS_KEY`
2. Environment variables with the same names

If neither source provides credentials, the query **fails with an error**
rather than silently falling back to anonymous access (which would turn
private buckets into hard-to-diagnose 403s). Use `CREDENTIALS_KIND='Anonymous'`
explicitly for public buckets.

### `Anonymous`

No credentials are sent (requests are unsigned). Use this mode for publicly accessible buckets.

### `Explicit`

Access key and secret key are specified directly in the query options; the
query fails if they are missing.

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

HTTP/HTTPS sources are **read-only** and must support both `HEAD` requests
(used to determine the file size) and `Range` requests (used for ranged
reads). A server that ignores `Range` and answers `200` is rejected with an
error instead of being silently misread.

Additional HTTP options:

| Option          | Type   | Default | Description                                        |
| --------------- | ------ | ------- | -------------------------------------------------- |
| `BEARER_TOKEN`  | string | —      | Sent as an `Authorization: Bearer <token>` header. |
| `HTTP_HEADERS`  | string | —      | Extra request headers.                             |
| `VERIFY_SSL`    | bool   | `true`  | Verify the server's TLS certificate.               |
| `CA_CERT_FILE`  | string | auto    | CA bundle used for TLS verification.               |
| `CONNECT_TIMEOUT` | double | `30.0` | Connection timeout in seconds.                    |
| `REQUEST_TIMEOUT` | double | `300.0` | Request timeout in seconds.                      |

## Export (COPY TO)

The HTTPFS Extension also supports writing query results to S3/OSS using `COPY TO`. This requires credentials with write permission (Anonymous mode cannot write). Objects larger than twice the multipart part size (16 MiB by default) are uploaded with a multipart upload; smaller objects use a single `PutObject`.

### Export to S3

```cypher
COPY (MATCH (n:Person) RETURN n.name, n.age)
TO "s3://my-bucket/output/person.csv" (
    CREDENTIALS_KIND='Default',
    OSS_ENDPOINT='oss-cn-beijing.aliyuncs.com'
);
```

### Export to OSS with Explicit Credentials

```cypher
COPY (MATCH (n:Person) RETURN n.name, n.age)
TO "oss://my-bucket/output/person.csv" (
    CREDENTIALS_KIND='Explicit',
    ENDPOINT_OVERRIDE='oss-cn-beijing.aliyuncs.com',
    OSS_ACCESS_KEY_ID='your-access-key-id',
    OSS_ACCESS_KEY_SECRET='your-access-key-secret'
);
```

> **Note:** HTTP/HTTPS endpoints are read-only and do not support `COPY TO`.

### Glob Pattern

Load multiple files matching a pattern. Supported wildcards: `*` (matches any sequence of characters), `?` (matches a single character), `[abc]` (matches any character in the set). Patterns like `**` and `{a,b}` are **not** supported.

```cypher
LOAD FROM "s3://my-bucket/data/*.parquet"
RETURN *;
```

## Combining with Other Extensions

The HTTPFS Extension provides the virtual filesystem (VFS) layer only. To load Parquet files from S3/OSS/HTTP, both extensions must be loaded:

```cypher
LOAD HTTPFS;
LOAD PARQUET;

LOAD FROM "oss://my-bucket/data/person.parquet" (
    CREDENTIALS_KIND='Anonymous',
    ENDPOINT_OVERRIDE='oss-cn-beijing.aliyuncs.com'
)
RETURN *;
```

> **Note:** All relational operations supported by `LOAD FROM` — including type conversion, WHERE filtering, aggregation, sorting, and limiting — work the same way with remote files. See the [LOAD FROM reference](../data_io/load_data) for the complete list of operations.

