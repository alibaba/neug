/**
 * Tests for the httpfs S3 extension (curl-based, no Arrow/AWS SDK).
 *
 * Unit tests (URI parsing, SigV4 signing, glob helpers, option building) run
 * offline. Integration tests (list/read/write against a real endpoint) only
 * run when OSS_ACCESS_KEY_ID + OSS_ACCESS_KEY_SECRET are set in the
 * environment.
 */

#include <glog/logging.h>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <random>
#include <string>
#include <vector>
#include "glob_utils.h"
#include "gtest/gtest.h"
#include "neug/utils/exception/exception.h"
#include "neug/utils/io/read/common/options.h"
#include "neug/utils/io/read/common/schema.h"
#include "s3_client.h"
#include "s3_filesystem.h"
#include "s3_options.h"
#include "s3_sigv4.h"

using neug::extension::s3::HasGlobWildcard;
using neug::extension::s3::LongestGlobPrefix;
using neug::extension::s3::MatchGlobPattern;
using neug::extension::s3::S3Client;
using neug::extension::s3::S3ClientConfig;
using neug::extension::s3::S3FileSystem;
using neug::extension::s3::S3OptionsBuilder;
using neug::extension::s3::S3URIComponents;
using neug::extension::s3::SignSigV4;
using neug::extension::s3::SigV4Credentials;
using neug::extension::s3::SigV4Request;
using neug::reader::FileSchema;

namespace {

std::string getEnvOrDefault(const char* name,
                            const std::string& default_value) {
  const char* value = std::getenv(name);
  return value ? std::string(value) : default_value;
}

FileSchema makeSchema(const std::string& path,
                      const neug::reader::options_t& options = {}) {
  FileSchema schema;
  schema.paths = {path};
  schema.options = options;
  return schema;
}

}  // namespace

// ============================================================================
// 1. URI Parsing Tests
// ============================================================================

TEST(S3URIParserTest, ParseValidURI) {
  auto components =
      S3URIComponents::parse("s3://my-bucket/path/to/file.parquet");
  EXPECT_EQ(components.scheme, "s3");
  EXPECT_EQ(components.bucket, "my-bucket");
  EXPECT_EQ(components.objectKey, "path/to/file.parquet");
  EXPECT_FALSE(components.hasGlob);
}

TEST(S3URIParserTest, ParseOSSURI) {
  auto components =
      S3URIComponents::parse("oss://my-bucket/path/to/file.parquet");
  EXPECT_EQ(components.scheme, "oss");
  EXPECT_EQ(components.bucket, "my-bucket");
  EXPECT_EQ(components.objectKey, "path/to/file.parquet");
}

TEST(S3URIParserTest, ParseURIWithoutKey) {
  auto components = S3URIComponents::parse("s3://my-bucket");
  EXPECT_EQ(components.bucket, "my-bucket");
  EXPECT_EQ(components.objectKey, "");
  EXPECT_FALSE(components.hasGlob);
}

TEST(S3URIParserTest, ParseURIWithGlobPattern) {
  auto components = S3URIComponents::parse("s3://my-bucket/data/*.parquet");
  EXPECT_EQ(components.bucket, "my-bucket");
  EXPECT_EQ(components.objectKey, "data/*.parquet");
  EXPECT_TRUE(components.hasGlob);
}

TEST(S3URIParserTest, ToURI) {
  auto components =
      S3URIComponents::parse("oss://my-bucket/path/to/file.parquet");
  EXPECT_EQ(components.toURI(), "oss://my-bucket/path/to/file.parquet");
}

TEST(S3URIParserTest, ParseFlexibleBarePath) {
  auto components = S3URIComponents::parseFlexible("my-bucket/key/file.csv");
  EXPECT_EQ(components.bucket, "my-bucket");
  EXPECT_EQ(components.objectKey, "key/file.csv");
}

TEST(S3URIParserTest, ParseFlexibleFullURI) {
  auto components =
      S3URIComponents::parseFlexible("s3://my-bucket/key/file.csv");
  EXPECT_EQ(components.bucket, "my-bucket");
  EXPECT_EQ(components.objectKey, "key/file.csv");
}

TEST(S3URIParserTest, InvalidSchemeThrows) {
  EXPECT_THROW(S3URIComponents::parse("ftp://bucket/key"),
               neug::exception::IOException);
  EXPECT_THROW(S3URIComponents::parse("bucket/key"),
               neug::exception::IOException);
}

// ============================================================================
// 2. Glob Helper Tests
// ============================================================================

TEST(GlobUtilsTest, MatchGlobPattern) {
  EXPECT_TRUE(MatchGlobPattern("data/2026/a.parquet", "data/2026/*.parquet"));
  EXPECT_FALSE(MatchGlobPattern("data/2026/b.csv", "data/2026/*.parquet"));
  EXPECT_TRUE(MatchGlobPattern("a.csv", "?.csv"));
  EXPECT_TRUE(MatchGlobPattern("data/x/y.parquet", "data/*.parquet"));
}

TEST(GlobUtilsTest, LongestGlobPrefix) {
  EXPECT_EQ(LongestGlobPrefix("data/2026/*.parquet"), "data/2026/");
  EXPECT_EQ(LongestGlobPrefix("data/*.parquet"), "data/");
  EXPECT_EQ(LongestGlobPrefix("*.parquet"), "");
  EXPECT_EQ(LongestGlobPrefix("data/file.parquet"), "data/file.parquet");
}

TEST(GlobUtilsTest, HasGlobWildcard) {
  EXPECT_TRUE(HasGlobWildcard("data/*.parquet"));
  EXPECT_TRUE(HasGlobWildcard("data/?.parquet"));
  EXPECT_TRUE(HasGlobWildcard("data/[abc].parquet"));
  EXPECT_FALSE(HasGlobWildcard("data/file.parquet"));
}

// ============================================================================
// 3. SigV4 Signing Tests (AWS official golden test vectors)
// ============================================================================

// https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-auth.html
// GET Object example.
TEST(SigV4Test, GetObjectGoldenVector) {
  // Note: this documented example uses the secret key variant with '/'
  // (not the '+B' variant from the general SigV4 docs).
  SigV4Credentials creds{"AKIAIOSFODNN7EXAMPLE",
                         "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"};
  SigV4Request req;
  req.method = "GET";
  req.host = "examplebucket.s3.amazonaws.com";
  req.canonical_uri = "/test.txt";
  req.extra_headers = {{"range", "bytes=0-9"}};
  req.payload_hash = neug::extension::s3::EmptyPayloadSHA256();

  // 2013-05-24T00:00:00Z
  std::time_t fixed_time = 1369353600;
  auto signed_req = SignSigV4(creds, "us-east-1", "s3", req, fixed_time);

  EXPECT_EQ(signed_req.amz_date, "20130524T000000Z");
  EXPECT_EQ(signed_req.date_stamp, "20130524");
  EXPECT_EQ(signed_req.authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/"
            "us-east-1/s3/aws4_request, "
            "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, "
            "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039"
            "c6036bdb41");

  // Regression: the Authorization header must be part of the outgoing header
  // set — the HTTP client sends signed_req.headers verbatim.
  bool found_auth = false;
  for (const auto& h : signed_req.headers) {
    if (h.first == "authorization") {
      found_auth = true;
      EXPECT_EQ(h.second, signed_req.authorization);
    }
  }
  EXPECT_TRUE(found_auth) << "Authorization header missing from headers";
}

// PUT Object example (payload "Welcome to Amazon S3.", key "test$file.text",
// extra Date + x-amz-storage-class headers).
TEST(SigV4Test, PutObjectGoldenVector) {
  SigV4Credentials creds{"AKIAIOSFODNN7EXAMPLE",
                         "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"};
  const std::string payload = "Welcome to Amazon S3.";
  SigV4Request req;
  req.method = "PUT";
  req.host = "examplebucket.s3.amazonaws.com";
  req.canonical_uri = "/test%24file.text";
  req.extra_headers = {
      {"date", "Fri, 24 May 2013 00:00:00 GMT"},
      {"x-amz-storage-class", "REDUCED_REDUNDANCY"},
  };
  req.payload_hash = neug::extension::s3::SHA256Hex(payload);

  std::time_t fixed_time = 1369353600;
  auto signed_req = SignSigV4(creds, "us-east-1", "s3", req, fixed_time);

  EXPECT_EQ(signed_req.authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/"
            "us-east-1/s3/aws4_request, "
            "SignedHeaders=date;host;x-amz-content-sha256;x-amz-date;"
            "x-amz-storage-class, "
            "Signature=98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971a"
            "f0ece108bd");
}

// An empty payload_hash must be resolved to the empty-payload SHA256 before
// signing, and the same resolved value must appear in both the
// x-amz-content-sha256 header and the canonical request; otherwise the
// signature cannot match. Verify by comparing against the GetObject golden
// vector above, which signs the identical request with an explicit hash.
TEST(SigV4Test, EmptyPayloadHashResolvedConsistently) {
  SigV4Credentials creds{"AKIAIOSFODNN7EXAMPLE",
                         "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"};
  SigV4Request req;
  req.method = "GET";
  req.host = "examplebucket.s3.amazonaws.com";
  req.canonical_uri = "/test.txt";
  req.extra_headers = {{"range", "bytes=0-9"}};
  req.payload_hash = "";  // let the signer resolve the fallback

  std::time_t fixed_time = 1369353600;
  auto signed_req = SignSigV4(creds, "us-east-1", "s3", req, fixed_time);

  // The outgoing x-amz-content-sha256 header must carry the resolved hash.
  bool found_hash_header = false;
  for (const auto& h : signed_req.headers) {
    if (h.first == "x-amz-content-sha256") {
      found_hash_header = true;
      EXPECT_EQ(h.second, neug::extension::s3::EmptyPayloadSHA256());
    }
  }
  EXPECT_TRUE(found_hash_header);

  // Same request as the GET golden vector -> the signature must be identical.
  EXPECT_EQ(signed_req.authorization,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/"
            "us-east-1/s3/aws4_request, "
            "SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, "
            "Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039"
            "c6036bdb41");
}

TEST(SigV4Test, AnonymousSkipsSigning) {
  SigV4Credentials creds;  // empty -> anonymous
  SigV4Request req;
  req.method = "GET";
  req.host = "examplebucket.s3.amazonaws.com";
  req.canonical_uri = "/test.txt";

  auto signed_req = SignSigV4(creds, "us-east-1", "s3", req, 1369353600);
  EXPECT_TRUE(signed_req.authorization.empty());
  for (const auto& h : signed_req.headers) {
    EXPECT_NE(h.first, "authorization")
        << "Anonymous requests must not carry an Authorization header";
  }
}

TEST(SigV4Test, UriEncode) {
  EXPECT_EQ(neug::extension::s3::UriEncode("a/b c.txt", false), "a/b%20c.txt");
  EXPECT_EQ(neug::extension::s3::UriEncode("a/b c.txt", true), "a%2Fb%20c.txt");
  EXPECT_EQ(neug::extension::s3::UriEncode("test$file.txt", false),
            "test%24file.txt");
}

// ============================================================================
// 4. Option Building Tests (offline)
// ============================================================================

TEST(S3OptionsBuilderTest, ExplicitCredentials) {
  neug::reader::options_t options;
  options["CREDENTIALS_KIND"] = "Explicit";
  options["OSS_ACCESS_KEY_ID"] = "my-access-key";
  options["OSS_ACCESS_KEY_SECRET"] = "my-secret-key";
  options["OSS_ENDPOINT"] = "oss-cn-beijing.aliyuncs.com";

  auto config =
      S3OptionsBuilder(makeSchema("oss://bucket/key", options)).build();
  EXPECT_EQ(config.access_key, "my-access-key");
  EXPECT_EQ(config.secret_key, "my-secret-key");
  EXPECT_FALSE(config.anonymous);
  EXPECT_EQ(config.endpoint, "oss-cn-beijing.aliyuncs.com");
  EXPECT_EQ(config.region, "oss-cn-beijing");  // auto-detected
  EXPECT_FALSE(config.path_style);             // OSS uses virtual hosting
}

TEST(S3OptionsBuilderTest, ExplicitCredentialsMissingThrows) {
  neug::reader::options_t options;
  options["CREDENTIALS_KIND"] = "Explicit";
  EXPECT_THROW(S3OptionsBuilder(makeSchema("s3://bucket/key", options)).build(),
               neug::exception::InvalidArgumentException);
}

TEST(S3OptionsBuilderTest, AnonymousKind) {
  neug::reader::options_t options;
  options["CREDENTIALS_KIND"] = "Anonymous";
  auto config =
      S3OptionsBuilder(makeSchema("s3://bucket/key", options)).build();
  EXPECT_TRUE(config.anonymous);
}

TEST(S3OptionsBuilderTest, DefaultWithoutCredsThrows) {
  // Save original environment values
  const char* orig_oss_ak = std::getenv("OSS_ACCESS_KEY_ID");
  const char* orig_oss_sk = std::getenv("OSS_ACCESS_KEY_SECRET");
  const char* orig_aws_ak = std::getenv("AWS_ACCESS_KEY_ID");
  const char* orig_aws_sk = std::getenv("AWS_SECRET_ACCESS_KEY");
  std::string saved_oss_ak = orig_oss_ak ? orig_oss_ak : "";
  std::string saved_oss_sk = orig_oss_sk ? orig_oss_sk : "";
  std::string saved_aws_ak = orig_aws_ak ? orig_aws_ak : "";
  std::string saved_aws_sk = orig_aws_sk ? orig_aws_sk : "";

  // Make sure no credentials leak in from the environment.
  ::unsetenv("OSS_ACCESS_KEY_ID");
  ::unsetenv("OSS_ACCESS_KEY_SECRET");
  ::unsetenv("AWS_ACCESS_KEY_ID");
  ::unsetenv("AWS_SECRET_ACCESS_KEY");

  // Default mode must fail loudly when no credentials are available:
  // silently downgrading to anonymous would break deployments that rely
  // on ~/.aws/credentials or IAM/ECS roles (no longer supported) with
  // hard-to-diagnose 403s.
  EXPECT_THROW(S3OptionsBuilder(makeSchema("s3://bucket/key")).build(),
               neug::exception::InvalidArgumentException);

  // Restore original environment
  if (!saved_oss_ak.empty())
    ::setenv("OSS_ACCESS_KEY_ID", saved_oss_ak.c_str(), 1);
  if (!saved_oss_sk.empty())
    ::setenv("OSS_ACCESS_KEY_SECRET", saved_oss_sk.c_str(), 1);
  if (!saved_aws_ak.empty())
    ::setenv("AWS_ACCESS_KEY_ID", saved_aws_ak.c_str(), 1);
  if (!saved_aws_sk.empty())
    ::setenv("AWS_SECRET_ACCESS_KEY", saved_aws_sk.c_str(), 1);
}

TEST(S3OptionsBuilderTest, DefaultReadsEnvironmentVariables) {
  // Save and clear OSS_ vars so they don't interfere with AWS_ test
  const char* orig_oss_ak = std::getenv("OSS_ACCESS_KEY_ID");
  const char* orig_oss_sk = std::getenv("OSS_ACCESS_KEY_SECRET");
  std::string saved_oss_ak = orig_oss_ak ? orig_oss_ak : "";
  std::string saved_oss_sk = orig_oss_sk ? orig_oss_sk : "";
  ::unsetenv("OSS_ACCESS_KEY_ID");
  ::unsetenv("OSS_ACCESS_KEY_SECRET");

  ::setenv("TEST_S3_AK", "env-access-key", 1);
  ::setenv("TEST_S3_SK", "env-secret-key", 1);
  // Point the builder at env creds via AWS alias keys.
  ::setenv("AWS_ACCESS_KEY_ID", "env-access-key", 1);
  ::setenv("AWS_SECRET_ACCESS_KEY", "env-secret-key", 1);

  auto config = S3OptionsBuilder(makeSchema("s3://bucket/key")).build();
  EXPECT_FALSE(config.anonymous);
  // Use EXPECT_TRUE to avoid printing credentials on test failure
  EXPECT_TRUE(config.access_key == "env-access-key") << "access_key mismatch";
  EXPECT_TRUE(config.secret_key == "env-secret-key") << "secret_key mismatch";

  ::unsetenv("AWS_ACCESS_KEY_ID");
  ::unsetenv("AWS_SECRET_ACCESS_KEY");
  ::unsetenv("TEST_S3_AK");
  ::unsetenv("TEST_S3_SK");

  // Restore OSS_ vars
  if (!saved_oss_ak.empty())
    ::setenv("OSS_ACCESS_KEY_ID", saved_oss_ak.c_str(), 1);
  if (!saved_oss_sk.empty())
    ::setenv("OSS_ACCESS_KEY_SECRET", saved_oss_sk.c_str(), 1);
}

TEST(S3OptionsBuilderTest, EndpointSchemeParsing) {
  neug::reader::options_t options;
  options["ENDPOINT_OVERRIDE"] = "http://localhost:9000";
  // This test targets endpoint parsing; opt out of credential resolution
  // (Default now fails fast when no credentials are configured).
  options["CREDENTIALS_KIND"] = "Anonymous";
  auto config =
      S3OptionsBuilder(makeSchema("s3://bucket/key", options)).build();
  EXPECT_EQ(config.endpoint, "localhost:9000");
  EXPECT_EQ(config.scheme, "http");
  EXPECT_TRUE(config.path_style);  // localhost -> path-style
}

TEST(S3OptionsBuilderTest, UnsupportedCredentialsKindThrows) {
  neug::reader::options_t options;
  options["CREDENTIALS_KIND"] = "Role";
  EXPECT_THROW(S3OptionsBuilder(makeSchema("s3://bucket/key", options)).build(),
               neug::exception::InvalidArgumentException);
}

// ============================================================================
// 5. FileSystem-level Tests (offline)
// ============================================================================

TEST(S3FileSystemTest, GlobDirectPathNoNetwork) {
  // A glob without wildcards must not touch the network at all.
  neug::reader::options_t options;
  options["CREDENTIALS_KIND"] = "Anonymous";
  S3FileSystem fs(makeSchema("s3://bucket/data/file.parquet", options));
  auto resolved = fs.glob("s3://bucket/data/file.parquet");
  ASSERT_EQ(resolved.size(), 1u);
  EXPECT_EQ(resolved[0], "s3://bucket/data/file.parquet");
}

TEST(S3FileSystemTest, RemoteFileSystemNonNull) {
  neug::reader::options_t options;
  options["CREDENTIALS_KIND"] = "Anonymous";
  S3FileSystem fs(makeSchema("s3://bucket/data/file.parquet", options));
  EXPECT_NE(fs.getRemoteFileSystem(), nullptr);
}

TEST(S3FileSystemTest, InvalidPathThrows) {
  EXPECT_THROW(S3FileSystem(makeSchema("ftp://bucket/key")),
               neug::exception::IOException);
}

// ============================================================================
// 6. Integration Tests (require real credentials in the environment)
// ============================================================================

class S3IntegrationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    access_key_ = getEnvOrDefault("OSS_ACCESS_KEY_ID", "");
    secret_key_ = getEnvOrDefault("OSS_ACCESS_KEY_SECRET", "");
    if (access_key_.empty() || secret_key_.empty()) {
      GTEST_SKIP() << "OSS_ACCESS_KEY_ID/OSS_ACCESS_KEY_SECRET not set; "
                      "skipping S3 integration tests";
    }
    endpoint_ = getEnvOrDefault("OSS_ENDPOINT", "oss-cn-beijing.aliyuncs.com");
    bucket_ = getEnvOrDefault("OSS_TEST_BUCKET", "graphscope");
    prefix_ = getEnvOrDefault("OSS_TEST_PREFIX", "httpfs_test/");
  }

  neug::reader::options_t baseOptions() {
    neug::reader::options_t options;
    options["OSS_ENDPOINT"] = endpoint_;
    return options;
  }

  std::string access_key_;
  std::string secret_key_;
  std::string endpoint_;
  std::string bucket_;
  std::string prefix_;
};

TEST_F(S3IntegrationTest, WriteListReadRoundTrip) {
  // Generate a deterministic pseudo-random payload (~1 MiB).
  std::string payload;
  payload.reserve(1 << 20);
  std::mt19937_64 rng(42);
  while (payload.size() < (1u << 20)) {
    uint64_t v = rng();
    payload.append(reinterpret_cast<const char*>(&v), sizeof(v));
  }

  const std::string key = prefix_ + "roundtrip.bin";
  const std::string uri = "oss://" + bucket_ + "/" + key;

  // Write via the RemoteFileSystem output stream (multipart path).
  S3FileSystem fs(makeSchema(uri, baseOptions()));
  auto remote = fs.getRemoteFileSystem();
  ASSERT_NE(remote, nullptr);

  auto out_result = remote->openOutputStream(uri);
  ASSERT_TRUE(out_result.has_value()) << out_result.error().ToString();
  auto out = *out_result;
  // Write in odd-sized chunks to exercise buffering.
  size_t offset = 0;
  const size_t chunk = 123457;
  while (offset < payload.size()) {
    size_t n = std::min(chunk, payload.size() - offset);
    auto w = out->Write(payload.data() + offset, static_cast<int64_t>(n));
    ASSERT_TRUE(w.has_value()) << w.error().ToString();
    offset += n;
  }
  auto close_status = out->Close();
  ASSERT_TRUE(close_status.has_value()) << close_status.error().ToString();

  // Existence + size.
  auto exists = remote->exists(uri);
  ASSERT_TRUE(exists.has_value()) << exists.error().ToString();
  EXPECT_TRUE(*exists);
  auto size = remote->getSize(uri);
  ASSERT_TRUE(size.has_value()) << size.error().ToString();
  EXPECT_EQ(*size, static_cast<int64_t>(payload.size()));

  // Read back via ranged reads and compare.
  auto in_result = remote->openInputStream(uri);
  ASSERT_TRUE(in_result.has_value()) << in_result.error().ToString();
  auto in = *in_result;

  std::string readback(payload.size(), '\0');
  int64_t pos = 0;
  const int64_t read_chunk = 65536;
  while (pos < static_cast<int64_t>(payload.size())) {
    int64_t n = std::min<int64_t>(read_chunk,
                                  static_cast<int64_t>(payload.size()) - pos);
    auto r = in->ReadAt(pos, n, readback.data() + pos);
    ASSERT_TRUE(r.has_value()) << r.error().ToString();
    ASSERT_EQ(*r, n) << "short read at offset " << pos;
    pos += n;
  }
  EXPECT_EQ(readback, payload);

  // Glob expansion should find the object.
  auto matched = fs.glob("oss://" + bucket_ + "/" + prefix_ + "roundtrip.*");
  ASSERT_EQ(matched.size(), 1u);
  EXPECT_EQ(matched[0], uri);
}

// A payload above the 2 * part_size threshold (default 16 MiB) forces the
// real multipart path: CreateMultipartUpload (?uploads), UploadPart per
// part and CompleteMultipartUpload. The round-trip readback validates every
// part landed at the right offset.
TEST_F(S3IntegrationTest, MultipartWriteAboveThresholdRoundTrip) {
  // Generate a deterministic pseudo-random payload (~20 MiB).
  constexpr size_t kPayloadSize = 20u << 20;
  std::string payload;
  payload.reserve(kPayloadSize);
  std::mt19937_64 rng(7);
  while (payload.size() < kPayloadSize) {
    uint64_t v = rng();
    payload.append(reinterpret_cast<const char*>(&v), sizeof(v));
  }

  const std::string key = prefix_ + "multipart_roundtrip.bin";
  const std::string uri = "oss://" + bucket_ + "/" + key;

  S3FileSystem fs(makeSchema(uri, baseOptions()));
  auto remote = fs.getRemoteFileSystem();
  ASSERT_NE(remote, nullptr);

  auto out_result = remote->openOutputStream(uri);
  ASSERT_TRUE(out_result.has_value()) << out_result.error().ToString();
  auto out = *out_result;
  // Odd-sized chunks so part boundaries fall mid-write.
  size_t offset = 0;
  const size_t chunk = 1048583;  // ~1 MiB, not a divisor of part_size
  while (offset < payload.size()) {
    size_t n = std::min(chunk, payload.size() - offset);
    auto w = out->Write(payload.data() + offset, static_cast<int64_t>(n));
    ASSERT_TRUE(w.has_value()) << w.error().ToString();
    offset += n;
  }
  auto close_status = out->Close();
  ASSERT_TRUE(close_status.has_value()) << close_status.error().ToString();

  auto size = remote->getSize(uri);
  ASSERT_TRUE(size.has_value()) << size.error().ToString();
  EXPECT_EQ(*size, static_cast<int64_t>(payload.size()));

  // Read back through the readahead-enabled stream and compare.
  auto in_result = remote->openInputStream(uri);
  ASSERT_TRUE(in_result.has_value()) << in_result.error().ToString();
  auto in = *in_result;

  std::string readback(payload.size(), '\0');
  int64_t pos = 0;
  const int64_t read_chunk = 1 << 20;
  while (pos < static_cast<int64_t>(payload.size())) {
    int64_t n = std::min<int64_t>(read_chunk,
                                  static_cast<int64_t>(payload.size()) - pos);
    auto r = in->ReadAt(pos, n, readback.data() + pos);
    ASSERT_TRUE(r.has_value()) << r.error().ToString();
    ASSERT_EQ(*r, n) << "short read at offset " << pos;
    pos += n;
  }
  EXPECT_EQ(readback, payload);
}
