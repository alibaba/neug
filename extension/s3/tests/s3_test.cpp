/**
 * OSS Integration tests for S3 extension
 */

#include "gtest/gtest.h"
#include "s3_filesystem.h"
#include "neug/utils/reader/schema.h"
#include "neug/utils/exception/exception.h"
#include <arrow/filesystem/s3fs.h>
#include <cstdlib>
#include <iomanip>

using neug::extension::s3::S3FileSystemProvider;
using neug::extension::s3::S3URIComponents;
using neug::reader::FileSchema;

// Global test environment to properly finalize S3
class S3TestEnvironment : public ::testing::Environment {
 public:
  ~S3TestEnvironment() override {}
  
  void TearDown() override {
    // Finalize Arrow S3 subsystem to avoid memory leaks and segfault warnings
    auto status = arrow::fs::FinalizeS3();
    if (!status.ok()) {
      std::cerr << "Warning: Failed to finalize S3: " << status.ToString() << std::endl;
    }
  }
};

// Register the global test environment
static ::testing::Environment* const s3_env =
    ::testing::AddGlobalTestEnvironment(new S3TestEnvironment);

class S3ExtensionTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Initialize Arrow S3 subsystem (required for buildS3Options tests)
    auto init_result = arrow::fs::EnsureS3Initialized();
    if (!init_result.ok()) {
      GTEST_SKIP() << "Failed to initialize Arrow S3: " << init_result.ToString();
    }
    
    oss_endpoint_ = getEnvOrDefault("OSS_ENDPOINT", "oss-cn-hangzhou.aliyuncs.com");
    oss_bucket_ = getEnvOrDefault("OSS_TEST_BUCKET", "neug");
    oss_access_key_ = getEnvOrDefault("OSS_ACCESS_KEY_ID", "");
    oss_secret_key_ = getEnvOrDefault("OSS_ACCESS_KEY_SECRET", "");
  }
  
  std::string getEnvOrDefault(const char* name, const std::string& default_value) {
    const char* value = std::getenv(name);
    return value ? std::string(value) : default_value;
  }
  
  std::string oss_endpoint_;
  std::string oss_bucket_;
  std::string oss_access_key_;
  std::string oss_secret_key_;
};

// ============================================================================
// 1. URI Parsing Tests
// ============================================================================

TEST(S3URIParserTest, ParseValidURI) {
  auto components = S3URIComponents::parse("s3://my-bucket/path/to/file.parquet");
  EXPECT_EQ(components.bucket, "my-bucket");
  EXPECT_EQ(components.objectKey, "path/to/file.parquet");
  EXPECT_FALSE(components.hasGlob);
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

TEST(S3URIParserTest, ParseURIWithMultipleGlobs) {
  auto components = S3URIComponents::parse("s3://my-bucket/*/data/?.csv");
  EXPECT_EQ(components.bucket, "my-bucket");
  EXPECT_EQ(components.objectKey, "*/data/?.csv");
  EXPECT_TRUE(components.hasGlob);
}

TEST(S3URIParserTest, ParseNestedPath) {
  auto components = S3URIComponents::parse(
      "s3://data-lake/year=2024/month=01/day=01/data.parquet");
  EXPECT_EQ(components.bucket, "data-lake");
  EXPECT_EQ(components.objectKey, "year=2024/month=01/day=01/data.parquet");
  EXPECT_FALSE(components.hasGlob);
}

TEST(S3URIParserTest, InvalidURIMissingScheme) {
  EXPECT_THROW({
    S3URIComponents::parse("http://my-bucket/file.parquet");
  }, neug::exception::Exception);
}

TEST(S3URIParserTest, InvalidURIMissingBucket) {
  EXPECT_THROW({
    S3URIComponents::parse("s3:///path/to/file");
  }, neug::exception::Exception);
}

TEST(S3URIParserTest, InvalidURIWrongFormat) {
  EXPECT_THROW({
    S3URIComponents::parse("s3:/bucket/file");
  }, neug::exception::Exception);
  EXPECT_THROW({
    S3URIComponents::parse("s3://");
  }, neug::exception::Exception);
}

TEST(S3URIParserTest, BucketNameValidation) {
  EXPECT_THROW({
    S3URIComponents::parse("s3://ab/file");
  }, neug::exception::Exception);
  
  std::string long_bucket(64, 'a');
  EXPECT_THROW({
    S3URIComponents::parse("s3://" + long_bucket + "/file");
  }, neug::exception::Exception);
}

TEST(S3URIParserTest, SpecialCharactersInKey) {
  auto components = S3URIComponents::parse(
      "s3://my-bucket/path/file%20with%20spaces.txt");
  EXPECT_EQ(components.bucket, "my-bucket");
  EXPECT_EQ(components.objectKey, "path/file%20with%20spaces.txt");
}

TEST(S3URIParserTest, RecursiveGlobPattern) {
  auto components = S3URIComponents::parse("s3://my-bucket/**/data.csv");
  EXPECT_EQ(components.bucket, "my-bucket");
  EXPECT_EQ(components.objectKey, "**/data.csv");
  EXPECT_TRUE(components.hasGlob);
}

// ============================================================================
// 2. S3Options Configuration Tests
// ============================================================================

TEST_F(S3ExtensionTest, BuildS3Options_DefaultAWS) {
  FileSchema schema;
  schema.paths = {"s3://test-bucket/file.parquet"};
  schema.protocol = "s3";
  // No endpoint specified - should use AWS defaults
  
  S3FileSystemProvider provider;
  auto s3_options = provider.buildS3Options(schema);
  
  EXPECT_TRUE(s3_options.endpoint_override.empty());  // Default AWS
  EXPECT_EQ(s3_options.region, "us-east-1");  // Default region
  EXPECT_EQ(s3_options.scheme, "https");
  EXPECT_FALSE(s3_options.force_virtual_addressing);  // AWS default
}

TEST_F(S3ExtensionTest, BuildS3Options_OSSEndpoint) {
  FileSchema schema;
  schema.paths = {"oss://test-bucket/file.parquet"};
  schema.protocol = "s3";
  schema.options["OSS_ENDPOINT"] = "oss-cn-beijing.aliyuncs.com";
  
  S3FileSystemProvider provider;
  auto s3_options = provider.buildS3Options(schema);
  
  EXPECT_EQ(s3_options.endpoint_override, "oss-cn-beijing.aliyuncs.com");
  EXPECT_EQ(s3_options.region, "oss-cn-beijing");  // Auto-detected
  EXPECT_EQ(s3_options.scheme, "https");
  EXPECT_TRUE(s3_options.force_virtual_addressing);  // OSS requires this
}

TEST_F(S3ExtensionTest, BuildS3Options_ExplicitRegion) {
  FileSchema schema;
  schema.paths = {"oss://test-bucket/file.parquet"};
  schema.protocol = "s3";
  schema.options["OSS_ENDPOINT"] = "oss-cn-beijing.aliyuncs.com";
  schema.options["OSS_REGION"] = "custom-region";
  
  S3FileSystemProvider provider;
  auto s3_options = provider.buildS3Options(schema);
  
  EXPECT_EQ(s3_options.region, "custom-region");  // Explicit region overrides auto-detect
}

TEST_F(S3ExtensionTest, BuildS3Options_HTTPSScheme) {
  FileSchema schema;
  schema.paths = {"s3://test-bucket/file.parquet"};
  schema.protocol = "s3";
  schema.options["OSS_ENDPOINT"] = "https://oss-cn-shanghai.aliyuncs.com";
  
  S3FileSystemProvider provider;
  auto s3_options = provider.buildS3Options(schema);
  
  EXPECT_EQ(s3_options.endpoint_override, "oss-cn-shanghai.aliyuncs.com");
  EXPECT_EQ(s3_options.scheme, "https");
}

TEST_F(S3ExtensionTest, BuildS3Options_AnonymousCredentials) {
  FileSchema schema;
  schema.paths = {"s3://test-bucket/file.parquet"};
  schema.protocol = "s3";
  schema.options["CREDENTIALS_KIND"] = "Anonymous";
  
  S3FileSystemProvider provider;
  auto s3_options = provider.buildS3Options(schema);
  
  EXPECT_EQ(s3_options.credentials_kind, arrow::fs::S3CredentialsKind::Anonymous);
}

TEST_F(S3ExtensionTest, BuildS3Options_ExplicitCredentials) {
  FileSchema schema;
  schema.paths = {"s3://test-bucket/file.parquet"};
  schema.protocol = "s3";
  schema.options["CREDENTIALS_KIND"] = "Explicit";
  schema.options["OSS_ACCESS_KEY_ID"] = "test-key";
  schema.options["OSS_ACCESS_KEY_SECRET"] = "test-secret";
  
  S3FileSystemProvider provider;
  auto s3_options = provider.buildS3Options(schema);
  
  EXPECT_EQ(s3_options.credentials_kind, arrow::fs::S3CredentialsKind::Explicit);
  EXPECT_EQ(s3_options.GetAccessKey(), "test-key");
  EXPECT_EQ(s3_options.GetSecretKey(), "test-secret");
}

TEST_F(S3ExtensionTest, BuildS3Options_AWSCredentialsAlias) {
  FileSchema schema;
  schema.paths = {"s3://test-bucket/file.parquet"};
  schema.protocol = "s3";
  schema.options["CREDENTIALS_KIND"] = "Explicit";
  schema.options["AWS_ACCESS_KEY_ID"] = "aws-key";
  schema.options["AWS_SECRET_ACCESS_KEY"] = "aws-secret";
  
  S3FileSystemProvider provider;
  auto s3_options = provider.buildS3Options(schema);
  
  EXPECT_EQ(s3_options.GetAccessKey(), "aws-key");
  EXPECT_EQ(s3_options.GetSecretKey(), "aws-secret");
}

TEST_F(S3ExtensionTest, BuildS3Options_MultiRegion) {
  std::vector<std::pair<std::string, std::string>> regions = {
    {"oss-cn-hangzhou.aliyuncs.com", "oss-cn-hangzhou"},
    {"oss-cn-beijing.aliyuncs.com", "oss-cn-beijing"},
    {"oss-cn-shanghai.aliyuncs.com", "oss-cn-shanghai"},
    {"oss-cn-shenzhen.aliyuncs.com", "oss-cn-shenzhen"}
  };
  
  for (const auto& [endpoint, expected_region] : regions) {
    FileSchema schema;
    schema.paths = {"oss://test/file.parquet"};
    schema.protocol = "s3";
    schema.options["OSS_ENDPOINT"] = endpoint;
    
    S3FileSystemProvider provider;
    auto s3_options = provider.buildS3Options(schema);
    
    EXPECT_EQ(s3_options.endpoint_override, endpoint);
    EXPECT_EQ(s3_options.region, expected_region);
    EXPECT_EQ(s3_options.scheme, "https");
    EXPECT_TRUE(s3_options.force_virtual_addressing);
  }
}

// ============================================================================
// 3. Path Resolution Tests (resolveS3Paths)
// ============================================================================

// Note: These tests require a mock S3FileSystem or real S3/OSS access
// For now, we focus on testing the parsing logic

TEST_F(S3ExtensionTest, ResolveS3Paths_SingleDirectPath) {
  // This test validates the URI parsing part of resolveS3Paths
  auto components = S3URIComponents::parse("s3://bucket/data/file.parquet");
  
  // Expected Arrow path format: "bucket/data/file.parquet"
  std::string expected_arrow_path = components.bucket;
  if (!components.objectKey.empty()) {
    expected_arrow_path += "/" + components.objectKey;
  }
  
  EXPECT_EQ(expected_arrow_path, "bucket/data/file.parquet");
  EXPECT_FALSE(components.hasGlob);
}

TEST_F(S3ExtensionTest, ResolveS3Paths_MultipleDirectPaths) {
  std::vector<std::string> paths = {
    "s3://bucket/file1.parquet",
    "s3://bucket/file2.parquet",
    "oss://bucket/file3.parquet"
  };
  
  std::vector<std::string> expected_arrow_paths;
  for (const auto& path : paths) {
    auto components = S3URIComponents::parse(path);
    std::string arrow_path = components.bucket;
    if (!components.objectKey.empty()) {
      arrow_path += "/" + components.objectKey;
    }
    expected_arrow_paths.push_back(arrow_path);
    EXPECT_FALSE(components.hasGlob);
  }
  
  EXPECT_EQ(expected_arrow_paths.size(), 3);
  EXPECT_EQ(expected_arrow_paths[0], "bucket/file1.parquet");
  EXPECT_EQ(expected_arrow_paths[1], "bucket/file2.parquet");
  EXPECT_EQ(expected_arrow_paths[2], "bucket/file3.parquet");
}

TEST_F(S3ExtensionTest, ResolveS3Paths_GlobPattern) {
  auto components = S3URIComponents::parse("s3://bucket/data/*.parquet");
  
  EXPECT_EQ(components.bucket, "bucket");
  EXPECT_EQ(components.objectKey, "data/*.parquet");
  EXPECT_TRUE(components.hasGlob);
}

TEST_F(S3ExtensionTest, ResolveS3Paths_NestedGlobPattern) {
  auto components = S3URIComponents::parse("oss://bucket/year=2024/month=*/data.parquet");
  
  EXPECT_EQ(components.bucket, "bucket");
  EXPECT_EQ(components.objectKey, "year=2024/month=*/data.parquet");
  EXPECT_TRUE(components.hasGlob);
}

TEST_F(S3ExtensionTest, ResolveS3Paths_BucketOnlyPath) {
  auto components = S3URIComponents::parse("s3://bucket");
  
  std::string arrow_path = components.bucket;
  if (!components.objectKey.empty()) {
    arrow_path += "/" + components.objectKey;
  }
  
  EXPECT_EQ(arrow_path, "bucket");
  EXPECT_FALSE(components.hasGlob);
}

// ============================================================================
// 4. End-to-End Integration Tests (Real OSS/S3 Access)
// ============================================================================

TEST_F(S3ExtensionTest, E2E_InitializeOSSWithAnonymousAccess) {
  // Test OSS public bucket with explicit anonymous access
  // Using GraphScope public dataset on OSS
  FileSchema schema;
  schema.paths = {"oss://graphscope/neug-dataset/tinysnb/vPerson.parquet"};
  schema.format = "parquet";
  schema.protocol = "s3";
  // Use AWS-compatible option names
  schema.options["ENDPOINT_OVERRIDE"] = "oss-cn-beijing.aliyuncs.com";
  schema.options["AWS_DEFAULT_REGION"] = "oss-cn-beijing";
  schema.options["CREDENTIALS_KIND"] = "Anonymous";
  
  S3FileSystemProvider provider;
  
  auto fileInfo = provider.provide(schema);
  EXPECT_NE(fileInfo.fileSystem, nullptr);
  EXPECT_FALSE(fileInfo.resolvedPaths.empty());
  
  auto file_path = fileInfo.resolvedPaths[0];
  // Note: resolvedPaths already returns Arrow-format paths ("bucket/key"), not "s3://" or "oss://"
  // So we can use it directly with Arrow's GetFileInfo()
  auto file_info_result = fileInfo.fileSystem->GetFileInfo(file_path);
  if (file_info_result.ok()) {
    auto info = *file_info_result;
    LOG(INFO) << "Test: File info retrieved successfully";
    LOG(INFO) << "  Type: " << (info.IsFile() ? "file" : "directory");
    LOG(INFO) << "  Size: " << info.size() << " bytes";
    EXPECT_TRUE(info.IsFile());
    EXPECT_GT(info.size(), 0);  // File should have content
    SUCCEED() << "OSS anonymous access works - file verified: " << info.size() << " bytes";
  } else {
    FAIL() << "Failed to get file info: " << file_info_result.status().ToString();
  }
}

TEST_F(S3ExtensionTest, E2E_InitializeAWSS3WithAnonymousAccess) {
  // Test with AWS S3 public bucket (no OSS, just AWS S3)
  // Using NOAA's public dataset which supports anonymous access
  FileSchema schema;
  schema.paths = {"s3://noaa-ghcn-pds/csv/by_year/1763.csv"}; // NOAA public dataset
  schema.format = "csv";
  schema.protocol = "s3";
  // Use AWS-compatible option names
  // IMPORTANT: Explicitly set empty endpoint to override any environment variables
  // This ensures we use default AWS S3 endpoint, not OSS
  schema.options["ENDPOINT_OVERRIDE"] = "";  // Force default AWS S3 endpoint
  schema.options["AWS_DEFAULT_REGION"] = "us-east-1";  // NOAA bucket is in us-east-1
  schema.options["CREDENTIALS_KIND"] = "Anonymous";
  
  S3FileSystemProvider provider;
  
  LOG(INFO) << "Test: Initializing AWS S3 with anonymous credentials...";
  auto fileInfo = provider.provide(schema);
  LOG(INFO) << "Test: AWS S3 FileSystem initialized successfully";
  EXPECT_NE(fileInfo.fileSystem, nullptr);
  EXPECT_FALSE(fileInfo.resolvedPaths.empty());
  
  // Verify we can actually get file info (proves connectivity)
  auto file_path = fileInfo.resolvedPaths[0];
  LOG(INFO) << "Test: Attempting to get file info for: " << file_path;
  
  // Note: resolvedPaths already returns Arrow-format paths ("bucket/key"), not "s3://bucket/key"
  // So we can use it directly with Arrow's GetFileInfo()
  auto file_info_result = fileInfo.fileSystem->GetFileInfo(file_path);
  if (file_info_result.ok()) {
    auto info = *file_info_result;
    LOG(INFO) << "Test: File info retrieved successfully";
    LOG(INFO) << "  Type: " << (info.IsFile() ? "file" : "directory");
    LOG(INFO) << "  Size: " << info.size() << " bytes";
    EXPECT_TRUE(info.IsFile());
    EXPECT_GT(info.size(), 0);  // File should have content
    SUCCEED() << "AWS S3 anonymous access works - file verified: " << info.size() << " bytes";
  } else {
    FAIL() << "Failed to get file info: " << file_info_result.status().ToString();
  }
}

TEST_F(S3ExtensionTest, E2E_InitializeOSSWithEnvVariables) {
  // Check credentials
  if (oss_access_key_.empty() || oss_secret_key_.empty()) {
    GTEST_SKIP() << "OSS credentials not configured. "
                 << "Set OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET environment variables to run this test.";
  }
  
  // Test OSS with Default credential mode (Arrow SDK credential chain)
  // This test uses environment variables for both endpoint and credentials:
  //   - ENDPOINT (for OSS endpoint)
  //   - OSS_ACCESS_KEY_ID and OSS_ACCESS_KEY_SECRET
  // Using neug private dataset on OSS (requires credentials)
  FileSchema schema;
  schema.paths = {"oss://neug/github_archive/2015-01-01-0.parquet"};
  schema.format = "parquet";
  schema.protocol = "s3";
  schema.options["ENDPOINT_OVERRIDE"] = "oss-cn-beijing.aliyuncs.com";

  S3FileSystemProvider provider;
  
  auto fileInfo = provider.provide(schema);
  EXPECT_NE(fileInfo.fileSystem, nullptr);
  EXPECT_FALSE(fileInfo.resolvedPaths.empty());
  
  auto file_path = fileInfo.resolvedPaths[0];
  // Note: resolvedPaths already returns Arrow-format paths ("bucket/key"), not "s3://" or "oss://"
  // So we can use it directly with Arrow's GetFileInfo()
  auto file_info_result = fileInfo.fileSystem->GetFileInfo(file_path);
  if (file_info_result.ok()) {
    auto info = *file_info_result;
    LOG(INFO) << "Test: File info retrieved successfully";
    LOG(INFO) << "  Type: " << (info.IsFile() ? "file" : "directory");
    LOG(INFO) << "  Size: " << info.size() << " bytes";
    EXPECT_TRUE(info.IsFile());
    EXPECT_GT(info.size(), 0);  // File should have content
    SUCCEED() << "OSS anonymous access works - file verified: " << info.size() << " bytes";
  } else {
    FAIL() << "Failed to get file info: " << file_info_result.status().ToString();
  }
}

// ============================================================================
// 5. OSS Public Parquet File Access Test
// ============================================================================

TEST_F(S3ExtensionTest, E2E_AccessOSSPublicParquetFile) {
  // Test accessing public Parquet file on OSS (GraphScope dataset)
  // URL: https://graphscope.oss-cn-beijing.aliyuncs.com/neug-dataset/GithubGraphTest/nodes_Actor.parquet
  // Size: 157,466 bytes (154 KB)
  
  FileSchema schema;
  schema.paths = {"oss://graphscope/neug-dataset/GithubGraphTest/nodes_Actor.parquet"};
  schema.format = "parquet";
  schema.protocol = "s3";
  // Use OSS_ENDPOINT (NeuG canonical name) for OSS endpoint configuration
  schema.options["OSS_ENDPOINT"] = "oss-cn-beijing.aliyuncs.com";
  schema.options["OSS_REGION"] = "oss-cn-beijing";
  schema.options["CREDENTIALS_KIND"] = "Anonymous";
  
  S3FileSystemProvider provider;
  
  LOG(INFO) << "Test: Initializing OSS access for public Parquet file...";
  auto fileInfo = provider.provide(schema);
  LOG(INFO) << "Test: OSS FileSystem initialized successfully";
  
  EXPECT_NE(fileInfo.fileSystem, nullptr);
  EXPECT_FALSE(fileInfo.resolvedPaths.empty());
  
  // Get file info
  auto file_path = fileInfo.resolvedPaths[0];
  LOG(INFO) << "Test: Attempting to access: " << file_path;
  
  auto file_info_result = fileInfo.fileSystem->GetFileInfo(file_path);
  ASSERT_TRUE(file_info_result.ok()) 
      << "Failed to get file info: " << file_info_result.status().ToString();
  
  auto info = *file_info_result;
  LOG(INFO) << "Test: File info retrieved successfully";
  LOG(INFO) << "  Path: " << info.path();
  LOG(INFO) << "  Type: " << (info.IsFile() ? "File" : "Directory");
  LOG(INFO) << "  Size: " << info.size() << " bytes";
  
  EXPECT_TRUE(info.IsFile());
  EXPECT_EQ(info.size(), 157466) << "Expected 157466 bytes (154 KB)";
  
  // Try to open and read the file
  auto file_result = fileInfo.fileSystem->OpenInputFile(file_path);
  ASSERT_TRUE(file_result.ok()) 
      << "Failed to open file: " << file_result.status().ToString();
  
  auto file = *file_result;
  auto size_result = file->GetSize();
  ASSERT_TRUE(size_result.ok());
  
  LOG(INFO) << "Test: File opened successfully";
  LOG(INFO) << "  Confirmed size: " << *size_result << " bytes";
  
  EXPECT_EQ(*size_result, 157466);
  
  // Read first 4 bytes to verify Parquet magic number
  auto buffer_result = file->Read(4);
  ASSERT_TRUE(buffer_result.ok()) 
      << "Failed to read file: " << buffer_result.status().ToString();
  
  auto buffer = *buffer_result;
  ASSERT_EQ(buffer->size(), 4);
  
  // Parquet magic number: "PAR1" (0x50 0x41 0x52 0x31)
  const uint8_t* data = buffer->data();
  LOG(INFO) << "Test: Read first 4 bytes successfully";
  LOG(INFO) << "  Magic bytes: "
            << std::hex << std::setfill('0')
            << std::setw(2) << (int)data[0] << " "
            << std::setw(2) << (int)data[1] << " "
            << std::setw(2) << (int)data[2] << " "
            << std::setw(2) << (int)data[3]
            << std::dec;
  
  EXPECT_EQ(data[0], 0x50);  // 'P'
  EXPECT_EQ(data[1], 0x41);  // 'A'
  EXPECT_EQ(data[2], 0x52);  // 'R'
  EXPECT_EQ(data[3], 0x31);  // '1'
  
  LOG(INFO) << "✓ OSS public Parquet file access successful";
  LOG(INFO) << "  - File size verified: 157466 bytes";
  LOG(INFO) << "  - Parquet magic number verified: PAR1";
  LOG(INFO) << "  - S3FileSystem can access OSS public data";
}

// ============================================================================
// 6. Negative Test: HTTPS URL Cannot Be Accessed via S3FileSystem
// ============================================================================

TEST_F(S3ExtensionTest, E2E_HTTPSURLNotSupportedViaS3) {
  // This test demonstrates that S3FileSystem CANNOT access plain HTTPS URLs
  // URL: https://graphscope.oss-cn-beijing.aliyuncs.com/neug-dataset/GithubGraphTest/nodes_Actor.parquet
  // 
  // Why it fails:
  // 1. S3FileSystem sends S3 API requests (GetObject, ListBucket, etc.)
  // 2. Regular HTTPS servers don't implement S3 API
  // 3. Arrow 18.0.0 doesn't have HttpFileSystem
  // 
  // Correct approach: Use oss:// URI with OSS endpoint configuration
  
  FileSchema schema;
  // Try to use HTTPS URL directly (this should fail)
  schema.paths = {"https://graphscope.oss-cn-beijing.aliyuncs.com/neug-dataset/GithubGraphTest/nodes_Actor.parquet"};
  schema.format = "parquet";
  schema.protocol = "s3";  // Using S3 protocol with HTTPS URL
  schema.options["CREDENTIALS_KIND"] = "Anonymous";
  
  S3FileSystemProvider provider;
  
  LOG(INFO) << "Test: Attempting to access HTTPS URL via S3FileSystem (expected to fail)...";
  
  // This should either:
  // 1. Throw an exception during provide() due to invalid URI format
  // 2. Fail during GetFileInfo() because the path doesn't exist in S3 format
  
  try {
    auto fileInfo = provider.provide(schema);
    
    if (fileInfo.fileSystem != nullptr && !fileInfo.resolvedPaths.empty()) {
      auto file_path = fileInfo.resolvedPaths[0];
      LOG(INFO) << "Test: Resolved path: " << file_path;
      
      auto file_info_result = fileInfo.fileSystem->GetFileInfo(file_path);
      
      // If we get here, the request was made but should fail
      if (!file_info_result.ok()) {
        LOG(INFO) << "✓ As expected: S3 API request to HTTPS URL failed";
        LOG(INFO) << "  Error: " << file_info_result.status().ToString();
        SUCCEED() << "HTTPS URL correctly rejected by S3FileSystem";
      } else {
        // This would be very surprising - S3FileSystem somehow accessed a non-S3 HTTPS URL
        auto info = *file_info_result;
        LOG(WARNING) << "Unexpected: File info retrieved successfully?";
        LOG(WARNING) << "  Type: " << (info.IsFile() ? "File" : "Directory");
        LOG(WARNING) << "  Size: " << info.size();
        FAIL() << "Unexpected success: S3FileSystem should not be able to access plain HTTPS URLs";
      }
    } else {
      LOG(INFO) << "✓ FileSystem initialization failed (as expected for HTTPS URL)";
      SUCCEED();
    }
  } catch (const neug::exception::Exception& e) {
    LOG(INFO) << "✓ As expected: Exception thrown when trying to use HTTPS URL";
    LOG(INFO) << "  Error: " << e.what();
    SUCCEED() << "HTTPS URL correctly rejected: " << e.what();
  } catch (const std::exception& e) {
    LOG(INFO) << "✓ As expected: Exception thrown";
    LOG(INFO) << "  Error: " << e.what();
    SUCCEED();
  }
  
  LOG(INFO) << "";
  LOG(INFO) << "=== Key Findings ===";
  LOG(INFO) << "✗ S3FileSystem CANNOT access plain HTTPS URLs";
  LOG(INFO) << "✓ Reason: S3FileSystem uses S3 API (GetObject, etc.)";
  LOG(INFO) << "✓ Solution: Use oss:// URI with OSS_ENDPOINT configuration";
  LOG(INFO) << "✓ Example: oss://bucket/path + OSS_ENDPOINT=oss-cn-beijing.aliyuncs.com";
}
