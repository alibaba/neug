/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "s3_sigv4.h"

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <memory>
#include <sstream>

#include "neug/utils/exception/exception.h"

namespace neug {
namespace extension {
namespace s3 {

namespace {

// RAII wrapper for EVP_MD_CTX (OpenSSL 3 compatible).
struct EvpDigestContext {
  EVP_MD_CTX* ctx = EVP_MD_CTX_new();
  EvpDigestContext() {
    if (!ctx || EVP_DigestInit_ex(ctx, EVP_sha256(), nullptr) != 1) {
      THROW_IO_EXCEPTION("Failed to initialize SHA256 context");
    }
  }
  ~EvpDigestContext() {
    if (ctx) {
      EVP_MD_CTX_free(ctx);
    }
  }
};

std::string HMACSHA256(const unsigned char* key, size_t key_len,
                       const std::string& data) {
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_len = 0;
  if (HMAC(EVP_sha256(), key, static_cast<int>(key_len),
           reinterpret_cast<const unsigned char*>(data.data()), data.size(),
           digest, &digest_len) == nullptr) {
    THROW_IO_EXCEPTION("HMAC-SHA256 computation failed");
  }
  return std::string(reinterpret_cast<char*>(digest), digest_len);
}

std::string toLower(std::string s) {
  std::transform(s.begin(), s.end(), s.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return s;
}

// Trim leading/trailing whitespace and collapse inner runs of spaces to a
// single space, per SigV4 canonical header rules.
std::string trimAndCollapse(const std::string& value) {
  std::string out;
  out.reserve(value.size());
  bool in_space = false;
  size_t begin = value.find_first_not_of(" \t");
  size_t end = value.find_last_not_of(" \t");
  if (begin == std::string::npos) {
    return out;
  }
  for (size_t i = begin; i <= end; ++i) {
    char c = value[i];
    if (c == ' ' || c == '\t') {
      if (!in_space) {
        out.push_back(' ');
        in_space = true;
      }
    } else {
      out.push_back(c);
      in_space = false;
    }
  }
  return out;
}

}  // namespace

std::string HexEncode(const unsigned char* data, size_t len) {
  static const char* kHexDigits = "0123456789abcdef";
  std::string out;
  out.resize(len * 2);
  for (size_t i = 0; i < len; ++i) {
    out[2 * i] = kHexDigits[data[i] >> 4];
    out[2 * i + 1] = kHexDigits[data[i] & 0x0F];
  }
  return out;
}

std::string SHA256Hex(const void* data, size_t len) {
  EvpDigestContext ctx;
  if (EVP_DigestUpdate(ctx.ctx, data, len) != 1) {
    THROW_IO_EXCEPTION("SHA256 update failed");
  }
  unsigned char digest[EVP_MAX_MD_SIZE];
  unsigned int digest_len = 0;
  if (EVP_DigestFinal_ex(ctx.ctx, digest, &digest_len) != 1) {
    THROW_IO_EXCEPTION("SHA256 finalize failed");
  }
  return HexEncode(digest, digest_len);
}

std::string UriEncode(const std::string& input, bool encode_slash) {
  std::ostringstream out;
  out.fill('0');
  out << std::hex;
  for (unsigned char c : input) {
    if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9') || c == '-' || c == '.' || c == '_' ||
        c == '~') {
      out << static_cast<char>(c);
    } else if (c == '/' && !encode_slash) {
      out << '/';
    } else {
      out << '%' << std::uppercase << std::setw(2) << static_cast<int>(c)
          << std::nouppercase;
    }
  }
  return out.str();
}

std::string Iso8601BasicFormat(std::time_t time_utc) {
  std::tm tm_utc{};
  gmtime_r(&time_utc, &tm_utc);
  char buf[32];
  std::strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm_utc);
  return std::string(buf);
}

SigV4SignedRequest SignSigV4(const SigV4Credentials& creds,
                             const std::string& region,
                             const std::string& service,
                             const SigV4Request& request,
                             std::time_t timestamp) {
  SigV4SignedRequest signed_req;

  std::time_t sign_time = (timestamp != 0) ? timestamp : std::time(nullptr);
  signed_req.amz_date = Iso8601BasicFormat(sign_time);
  signed_req.date_stamp = signed_req.amz_date.substr(0, 8);

  // Resolve the payload hash once: the x-amz-content-sha256 header and the
  // canonical request must carry the exact same value, otherwise the
  // signature is guaranteed to mismatch.
  const std::string payload_hash = request.payload_hash.empty()
                                       ? std::string(EmptyPayloadSHA256())
                                       : request.payload_hash;

  // Collect all headers to sign: host + x-amz-date + x-amz-content-sha256
  // + caller supplied extras. Lowercase names, trim/collapse values.
  std::vector<std::pair<std::string, std::string>> canonical_headers;
  canonical_headers.emplace_back("host", request.host);
  canonical_headers.emplace_back("x-amz-content-sha256", payload_hash);
  canonical_headers.emplace_back("x-amz-date", signed_req.amz_date);
  for (const auto& h : request.extra_headers) {
    canonical_headers.emplace_back(toLower(h.first), trimAndCollapse(h.second));
  }
  std::sort(canonical_headers.begin(), canonical_headers.end());

  // SignedHeaders list: semicolon-joined lowercase sorted header names.
  std::string signed_headers;
  for (size_t i = 0; i < canonical_headers.size(); ++i) {
    if (i > 0) {
      signed_headers += ';';
    }
    signed_headers += canonical_headers[i].first;
  }

  // Canonical headers block: "name:value\n" per header.
  std::string canonical_headers_block;
  for (const auto& h : canonical_headers) {
    canonical_headers_block += h.first;
    canonical_headers_block += ':';
    canonical_headers_block += h.second;
    canonical_headers_block += '\n';
  }

  // Canonical query string: sorted by encoded key then encoded value.
  std::vector<std::pair<std::string, std::string>> encoded_params;
  encoded_params.reserve(request.query_params.size());
  for (const auto& p : request.query_params) {
    encoded_params.emplace_back(UriEncode(p.first, true),
                                UriEncode(p.second, true));
  }
  std::sort(encoded_params.begin(), encoded_params.end());
  std::string canonical_query;
  for (size_t i = 0; i < encoded_params.size(); ++i) {
    if (i > 0) {
      canonical_query += '&';
    }
    canonical_query += encoded_params[i].first;
    canonical_query += '=';
    canonical_query += encoded_params[i].second;
  }

  // Canonical URI: must start with '/'; S3 does not normalize '.' segments.
  std::string canonical_uri = request.canonical_uri;
  if (canonical_uri.empty() || canonical_uri[0] != '/') {
    canonical_uri = "/" + canonical_uri;
  }

  // Anonymous mode: no credentials -> skip signing entirely.
  if (creds.empty()) {
    signed_req.headers = canonical_headers;
    return signed_req;
  }

  // Step 1: CanonicalRequest
  std::string canonical_request;
  canonical_request.reserve(512);
  canonical_request += request.method;
  canonical_request += '\n';
  canonical_request += canonical_uri;
  canonical_request += '\n';
  canonical_request += canonical_query;
  canonical_request += '\n';
  canonical_request += canonical_headers_block;
  canonical_request += '\n';
  canonical_request += signed_headers;
  canonical_request += '\n';
  canonical_request += payload_hash;

  // Step 2: StringToSign
  const std::string credential_scope =
      signed_req.date_stamp + "/" + region + "/" + service + "/aws4_request";
  std::string string_to_sign;
  string_to_sign += "AWS4-HMAC-SHA256\n";
  string_to_sign += signed_req.amz_date;
  string_to_sign += '\n';
  string_to_sign += credential_scope;
  string_to_sign += '\n';
  string_to_sign += SHA256Hex(canonical_request);

  // Step 3: SigningKey = HMAC(HMAC(HMAC(HMAC("AWS4"+secret, date),
  //                                        region), service), "aws4_request")
  const std::string k_secret = "AWS4" + creds.secret_key;
  const std::string k_date =
      HMACSHA256(reinterpret_cast<const unsigned char*>(k_secret.data()),
                 k_secret.size(), signed_req.date_stamp);
  const std::string k_region =
      HMACSHA256(reinterpret_cast<const unsigned char*>(k_date.data()),
                 k_date.size(), region);
  const std::string k_service =
      HMACSHA256(reinterpret_cast<const unsigned char*>(k_region.data()),
                 k_region.size(), service);
  const std::string k_signing =
      HMACSHA256(reinterpret_cast<const unsigned char*>(k_service.data()),
                 k_service.size(), "aws4_request");

  // Step 4: Signature
  const std::string signature_raw =
      HMACSHA256(reinterpret_cast<const unsigned char*>(k_signing.data()),
                 k_signing.size(), string_to_sign);
  const std::string signature =
      HexEncode(reinterpret_cast<const unsigned char*>(signature_raw.data()),
                signature_raw.size());

  signed_req.authorization = "AWS4-HMAC-SHA256 Credential=" + creds.access_key +
                             "/" + credential_scope +
                             ", SignedHeaders=" + signed_headers +
                             ", Signature=" + signature;
  signed_req.headers = canonical_headers;
  // The Authorization header must be part of the outgoing header set; the
  // caller sends signed_req.headers verbatim.
  signed_req.headers.emplace_back("authorization", signed_req.authorization);
  return signed_req;
}

}  // namespace s3
}  // namespace extension
}  // namespace neug
