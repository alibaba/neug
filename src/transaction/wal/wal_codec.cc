/** Copyright 2020 Alibaba Group Holding Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * 	http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "neug/transaction/wal/wal_codec.h"

#include <cstring>

#include "absl/crc/crc32c.h"

namespace neug {

namespace {

// Little-endian primitives are serialized byte-by-byte; the codec never
// depends on host endianness or struct layout.
void PutU32(uint8_t* dst, uint32_t value) {
  for (int i = 0; i < 4; ++i) {
    dst[i] = static_cast<uint8_t>(value >> (8 * i));
  }
}

void PutU64(uint8_t* dst, uint64_t value) {
  for (int i = 0; i < 8; ++i) {
    dst[i] = static_cast<uint8_t>(value >> (8 * i));
  }
}

uint32_t GetU32(const uint8_t* src) {
  uint32_t value = 0;
  for (int i = 0; i < 4; ++i) {
    value |= static_cast<uint32_t>(src[i]) << (8 * i);
  }
  return value;
}

uint64_t GetU64(const uint8_t* src) {
  uint64_t value = 0;
  for (int i = 0; i < 8; ++i) {
    value |= static_cast<uint64_t>(src[i]) << (8 * i);
  }
  return value;
}

bool IsKnownRecordKind(uint32_t value) {
  return value == static_cast<uint32_t>(WalRecordKind::kInsert) ||
         value == static_cast<uint32_t>(WalRecordKind::kCowUpdate) ||
         value == static_cast<uint32_t>(WalRecordKind::kCompact);
}

constexpr size_t kWalTrailerPrefixSize =
    kWalFrameTrailerSize - sizeof(uint32_t);

/// Wire bytes of the trailer preceding its checksum field: the commit marker.
std::array<uint8_t, kWalTrailerPrefixSize> EncodeWalTrailerPrefix(
    const WalFrameTrailer& trailer) {
  std::array<uint8_t, kWalTrailerPrefixSize> bytes{};
  PutU32(bytes.data() + 0, trailer.commit_marker);
  return bytes;
}

/// Single source of truth for what the frame checksum covers: the encoded
/// frame header bytes, the payload, and the commit marker. This protects the
/// kind, timestamp and length fields, not only the payload. CRC32C is
/// hardware-accelerated on x86 (SSE4.2) and ARM64 via absl.
uint32_t WalFrameChecksum(const uint8_t* frame_header_bytes,
                          const uint8_t* payload, size_t payload_length,
                          const uint8_t* trailer_prefix) {
  absl::crc32c_t crc = absl::ExtendCrc32c(
      absl::crc32c_t{0},
      absl::string_view(reinterpret_cast<const char*>(frame_header_bytes),
                        kWalFrameHeaderStableSize));
  crc = absl::ExtendCrc32c(
      crc, absl::string_view(reinterpret_cast<const char*>(payload),
                             payload_length));
  crc = absl::ExtendCrc32c(
      crc, absl::string_view(reinterpret_cast<const char*>(trailer_prefix),
                             kWalTrailerPrefixSize));
  return static_cast<uint32_t>(crc);
}

}  // namespace

std::string WalRecordKindName(WalRecordKind kind) {
  switch (kind) {
  case WalRecordKind::kInsert:
    return "kInsert";
  case WalRecordKind::kCowUpdate:
    return "kCowUpdate";
  case WalRecordKind::kCompact:
    return "kCompact";
  }
  return "kUnknown(" + std::to_string(static_cast<uint32_t>(kind)) + ")";
}

std::string WalRecoveryErrorKindName(WalRecoveryErrorKind kind) {
  switch (kind) {
  case WalRecoveryErrorKind::kUnsupportedFormat:
    return "unsupported_format";
  case WalRecoveryErrorKind::kCorruptedFrame:
    return "corrupted_frame";
  case WalRecoveryErrorKind::kDuplicateTimestamp:
    return "duplicate_timestamp";
  case WalRecoveryErrorKind::kUnknownRecordKind:
    return "unknown_record_kind";
  }
  return "unknown";
}

std::string WalDecodeStatusName(WalDecodeStatus status) {
  switch (status) {
  case WalDecodeStatus::kOk:
    return "ok";
  case WalDecodeStatus::kTruncated:
    return "truncated";
  case WalDecodeStatus::kBadMagic:
    return "bad_magic";
  case WalDecodeStatus::kBadVersion:
    return "bad_version";
  case WalDecodeStatus::kBadHeaderSize:
    return "bad_header_size";
  case WalDecodeStatus::kBadChecksum:
    return "bad_checksum";
  case WalDecodeStatus::kUnknownRecordKind:
    return "unknown_record_kind";
  case WalDecodeStatus::kPayloadTooLarge:
    return "payload_too_large";
  }
  return "unknown";
}

std::array<uint8_t, kWalFileHeaderSize> EncodeWalFileHeader(
    const WalFileHeader& header) {
  std::array<uint8_t, kWalFileHeaderSize> bytes{};
  PutU32(bytes.data() + 0, header.magic);
  PutU32(bytes.data() + 4, header.format_version);
  PutU32(bytes.data() + 8, header.header_size);
  PutU32(bytes.data() + 12, header.writer_slot_id);
  PutU64(bytes.data() + 16, header.reserved);
  return bytes;
}

std::array<uint8_t, kWalFrameHeaderSize> EncodeWalFrameHeader(
    const WalFrameHeader& header) {
  std::array<uint8_t, kWalFrameHeaderSize> bytes{};
  PutU32(bytes.data() + 0, header.frame_magic);
  PutU32(bytes.data() + 4, static_cast<uint32_t>(header.record_kind));
  PutU32(bytes.data() + 8, header.header_size);
  PutU32(bytes.data() + 12, header.commit_timestamp);
  PutU64(bytes.data() + 16, header.payload_length);
  return bytes;
}

std::array<uint8_t, kWalFrameTrailerSize> EncodeWalFrameTrailer(
    const WalFrameTrailer& trailer, const uint8_t* frame_header_bytes,
    const uint8_t* payload, size_t payload_length) {
  std::array<uint8_t, kWalFrameTrailerSize> bytes{};
  const auto prefix = EncodeWalTrailerPrefix(trailer);
  std::memcpy(bytes.data(), prefix.data(), prefix.size());
  PutU32(bytes.data() + prefix.size(),
         WalFrameChecksum(frame_header_bytes, payload, payload_length,
                          prefix.data()));
  return bytes;
}

WalDecodeStatus DecodeWalFileHeader(const uint8_t* data, size_t remaining,
                                    WalFileHeader& out, size_t& consumed) {
  consumed = 0;
  if (remaining < kWalFileHeaderSize) {
    return WalDecodeStatus::kTruncated;
  }
  out.magic = GetU32(data + 0);
  if (out.magic != kWalFileMagic) {
    return WalDecodeStatus::kBadMagic;
  }
  out.format_version = GetU32(data + 4);
  if (out.format_version != kWalFormatVersion) {
    return WalDecodeStatus::kBadVersion;
  }
  out.header_size = GetU32(data + 8);
  if (out.header_size != kWalFileHeaderSize) {
    return WalDecodeStatus::kBadHeaderSize;
  }
  out.writer_slot_id = GetU32(data + 12);
  out.reserved = GetU64(data + 16);
  consumed = kWalFileHeaderSize;
  return WalDecodeStatus::kOk;
}

WalDecodeStatus DecodeWalFrameHeader(const uint8_t* data, size_t remaining,
                                     WalFrameHeader& out, size_t& consumed) {
  consumed = 0;
  if (remaining < kWalFrameHeaderSize) {
    return WalDecodeStatus::kTruncated;
  }
  out.frame_magic = GetU32(data + 0);
  if (out.frame_magic != kWalFrameMagic) {
    return WalDecodeStatus::kBadMagic;
  }
  const uint32_t kind = GetU32(data + 4);
  if (!IsKnownRecordKind(kind)) {
    return WalDecodeStatus::kUnknownRecordKind;
  }
  out.record_kind = static_cast<WalRecordKind>(kind);
  out.header_size = GetU32(data + 8);
  if (out.header_size != kWalFrameHeaderSize) {
    return WalDecodeStatus::kBadHeaderSize;
  }
  out.commit_timestamp = GetU32(data + 12);
  out.payload_length = GetU64(data + 16);
  if (out.payload_length > kWalMaxPayloadLength) {
    return WalDecodeStatus::kPayloadTooLarge;
  }
  consumed = kWalFrameHeaderSize;
  return WalDecodeStatus::kOk;
}

WalDecodeStatus DecodeWalFrameTrailer(const uint8_t* data, size_t remaining,
                                      WalFrameTrailer& out, size_t& consumed) {
  consumed = 0;
  if (remaining < kWalFrameTrailerSize) {
    return WalDecodeStatus::kTruncated;
  }
  out.commit_marker = GetU32(data + 0);
  if (out.commit_marker != kWalCommitMarker) {
    return WalDecodeStatus::kBadMagic;
  }
  out.frame_checksum = GetU32(data + 4);
  consumed = kWalFrameTrailerSize;
  return WalDecodeStatus::kOk;
}

WalDecodeStatus ValidateWalFrame(const WalFrameHeader& header,
                                 const WalFrameTrailer& trailer,
                                 const uint8_t* frame_header_bytes,
                                 const uint8_t* payload) {
  const auto prefix = EncodeWalTrailerPrefix(trailer);
  if (trailer.frame_checksum !=
      WalFrameChecksum(frame_header_bytes, payload,
                       static_cast<size_t>(header.payload_length),
                       prefix.data())) {
    return WalDecodeStatus::kBadChecksum;
  }
  return WalDecodeStatus::kOk;
}

}  // namespace neug
