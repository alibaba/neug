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
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <array>
#include <string>

namespace neug {

// =============================================================================
// WAL v1 on-disk protocol.
//
// Every multi-byte integer is little-endian. The byte layout below is a wire
// protocol, not a C++ object layout: encoding/decoding always goes through
// the explicit codec functions in this header and never through
// reinterpret_cast or struct dumps.
//
// File layout:
//   WalFileHeader | (WalFrameHeader | payload | WalFrameTrailer)*
//
// A file without a valid WalFileHeader is never silently interpreted; the
// legacy pre-v1 format is rejected with a typed recovery error.
// =============================================================================

// "NEUW" / "NEUF" / "NEUM" as little-endian u32 constants.
constexpr uint32_t kWalFileMagic = 0x5755454Eu;
constexpr uint32_t kWalFrameMagic = 0x4655454Eu;
constexpr uint32_t kWalCommitMarker = 0x4D55454Eu;

constexpr uint32_t kWalFormatVersion = 1;

constexpr uint32_t kWalFileHeaderSize = 24;
constexpr uint32_t kWalFrameHeaderSize = 24;
constexpr uint32_t kWalFrameTrailerSize = 8;

// Protocol upper bound for a frame payload. Checked before any conversion to
// size_t so corrupted values cannot trigger integer overflow, out-of-bounds
// reads, or huge allocations.
constexpr uint64_t kWalMaxPayloadLength = 1ULL << 32;

/// Explicit record categories. Unknown values are always rejected; semantics
/// are never inferred from payload length or bit flags.
enum class WalRecordKind : uint32_t {
  kInsert = 1,
  kCowUpdate = 2,
  kCompact = 3,
};

/// Typed WAL recovery error categories. Messages produced alongside these
/// must include the WAL path, the byte offset and the timestamp so operators
/// can locate the problem.
enum class WalRecoveryErrorKind {
  kUnsupportedFormat,
  kCorruptedFrame,
  kDuplicateTimestamp,
  kUnknownRecordKind,
};

std::string WalRecordKindName(WalRecordKind kind);
std::string WalRecoveryErrorKindName(WalRecoveryErrorKind kind);

/// Per-file fixed header. A WAL file belongs to the checkpoint whose
/// wal_dir() it lives in; that ownership is guaranteed by checkpoint
/// rotation, not by a field inside the file.
///
/// The structural fields (magic, format_version, header_size) are validated
/// by exact match on decode, so no header checksum is needed: only the
/// diagnostic writer_slot_id and reserved bytes would be protected.
///
/// Wire layout (little-endian):
///   magic u32 | format_version u32 | header_size u32 |
///   writer_slot_id u32 | reserved u64
struct WalFileHeader {
  uint32_t magic{kWalFileMagic};
  uint32_t format_version{kWalFormatVersion};
  uint32_t header_size{kWalFileHeaderSize};
  uint32_t writer_slot_id{0};
  uint64_t reserved{0};
};

/// Per-transaction frame header, written before the payload.
///
/// The format version is a file-level property carried by WalFileHeader; a
/// single writer file never mixes frame versions, so no per-frame version is
/// stored. The payload is protected by the trailer frame_checksum.
///
/// Wire layout (little-endian):
///   frame_magic u32 | record_kind u32 | header_size u32 |
///   commit_timestamp u32 | payload_length u64
struct WalFrameHeader {
  uint32_t frame_magic{kWalFrameMagic};
  WalRecordKind record_kind{WalRecordKind::kInsert};
  uint32_t header_size{kWalFrameHeaderSize};
  uint32_t commit_timestamp{0};
  uint64_t payload_length{0};
};

/// Commit trailer, written after the payload. The commit marker is only
/// persisted once the whole payload has been persisted. The timestamp and
/// length already live in the frame header and are covered by frame_checksum,
/// so duplicating them here would add no detection strength.
///
/// Wire layout (little-endian):
///   commit_marker u32 | frame_checksum u32
struct WalFrameTrailer {
  uint32_t commit_marker{kWalCommitMarker};
  /// CRC32C (Castagnani, hardware-accelerated via absl) over the encoded
  /// frame header, the payload, and the commit marker.
  uint32_t frame_checksum{0};
};

/// Status of a decode step. Decode functions never read past @c remaining.
enum class WalDecodeStatus {
  kOk = 0,
  /// Fewer bytes available than the structure needs. Only meaningful for the
  /// last candidate frame of a file (crash residue).
  kTruncated,
  kBadMagic,
  kBadVersion,
  kBadHeaderSize,
  kBadChecksum,
  kUnknownRecordKind,
  kPayloadTooLarge,
};

std::string WalDecodeStatusName(WalDecodeStatus status);

/// Encoders. All checksum fields are computed here; callers never fill them.
std::array<uint8_t, kWalFileHeaderSize> EncodeWalFileHeader(
    const WalFileHeader& header);
std::array<uint8_t, kWalFrameHeaderSize> EncodeWalFrameHeader(
    const WalFrameHeader& header);
/// @p frame_checksum covers the encoded frame header bytes, the payload, and
/// the commit marker. This protects the kind, timestamp and length fields,
/// not only the payload.
std::array<uint8_t, kWalFrameTrailerSize> EncodeWalFrameTrailer(
    const WalFrameTrailer& trailer, const uint8_t* frame_header_bytes,
    const uint8_t* payload, size_t payload_length);

/// Decoders. Each returns the decode status and, on success, fills @p out and
/// reports the consumed byte count.
WalDecodeStatus DecodeWalFileHeader(const uint8_t* data, size_t remaining,
                                    WalFileHeader& out, size_t& consumed);
WalDecodeStatus DecodeWalFrameHeader(const uint8_t* data, size_t remaining,
                                     WalFrameHeader& out, size_t& consumed);
WalDecodeStatus DecodeWalFrameTrailer(const uint8_t* data, size_t remaining,
                                      WalFrameTrailer& out, size_t& consumed);

/// Validates the trailer against the frame it closes: recomputes the frame
/// checksum over the header bytes, payload and commit marker. Returns kOk on
/// success.
WalDecodeStatus ValidateWalFrame(const WalFrameHeader& header,
                                 const WalFrameTrailer& trailer,
                                 const uint8_t* frame_header_bytes,
                                 const uint8_t* payload);

/// The frame header carries no checksum fields, so every encoded byte is
/// covered by frame_checksum.
constexpr size_t kWalFrameHeaderStableSize = kWalFrameHeaderSize;

}  // namespace neug
