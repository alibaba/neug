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
// The framing layer is encoded/decoded byte-by-byte through the explicit
// codec functions in this header; it never goes through reinterpret_cast,
// C++ bit-fields or struct dumps. This is a parse-safety property (bounds
// checks, rejection of invalid values), not cross-platform portability: the
// redo payload and the data files are persisted in host layout, so a WAL
// file is only meaningful on the machine that wrote it.
//
// File layout:
//   WalFileHeader | (WalFrameHeader | payload)*
//
// The frame checksum lives in the frame header and covers the remaining
// header bytes plus the payload. A frame is therefore complete exactly when
// it is fully present and its checksum matches; recovery never needs a
// trailing commit marker:
//   - EOF with fewer bytes than the frame needs  -> torn tail, discarded
//   - full frame, checksum matches               -> committed, replayed
//   - full frame, checksum mismatch              -> corruption, rejected
//
// A file without a valid WalFileHeader is never silently interpreted; the
// legacy pre-v1 format is rejected with a typed recovery error.
// =============================================================================

// "NEUW" as a little-endian u32 constant (codec implementation detail).
constexpr uint32_t kWalFileMagic = 0x5755454Eu;

constexpr uint32_t kWalFormatVersion = 1;

constexpr uint32_t kWalFileHeaderSize = 16;
constexpr uint32_t kWalFrameHeaderSize = 13;

// Protocol upper bound for a frame payload: the on-wire length field is a
// u32. Checked before any conversion to size_t so corrupted values cannot
// trigger integer overflow, out-of-bounds reads, or huge allocations.
constexpr uint64_t kWalMaxPayloadLength = 0xFFFFFFFFull;

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
/// rotation, not by a field inside the file. The writer identity is already
/// carried by the file name (thread_<slot>_<version>.wal), so the header
/// keeps no diagnostic slot field.
///
/// All fields are validated by exact match on decode, so no header checksum
/// is needed: only the reserved bytes would be protected.
///
/// Wire layout (little-endian):
///   magic u32 | format_version u32 | header_size u32 | reserved u32
struct WalFileHeader {
  uint32_t magic{kWalFileMagic};
  uint32_t format_version{kWalFormatVersion};
  uint32_t header_size{kWalFileHeaderSize};
  uint32_t reserved{0};
};

/// Per-transaction frame header, written before the payload. The frame
/// checksum is part of the header, so the writer computes it over the
/// payload before writing anything.
///
/// The format version is a file-level property carried by WalFileHeader; a
/// single writer file never mixes frame versions, so no per-frame version is
/// stored. There is no per-frame magic either: frames are parsed strictly
/// sequentially from a validated file header, and the checksum rejects any
/// corrupted frame regardless of its position.
///
/// Wire layout (little-endian, fields packed in order; padding is only ever
/// appended at the end, never between fields):
///   record_kind u8 | payload_length u32 | commit_timestamp u32 |
///   frame_checksum u32
struct WalFrameHeader {
  WalRecordKind record_kind{WalRecordKind::kInsert};
  uint32_t payload_length{0};
  uint32_t commit_timestamp{0};
  /// CRC32C (Castagnani, hardware-accelerated via absl) over the encoded
  /// header bytes preceding this field and the payload. Computed by
  /// EncodeWalFrameHeader; callers never fill it.
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
/// Computes @c frame_checksum over the encoded header bytes preceding the
/// checksum field plus the payload, protecting the kind, timestamp and
/// length fields, not only the payload.
std::array<uint8_t, kWalFrameHeaderSize> EncodeWalFrameHeader(
    const WalFrameHeader& header, const uint8_t* payload,
    size_t payload_length);

/// Decoders. Each returns the decode status and, on success, fills @p out and
/// reports the consumed byte count.
WalDecodeStatus DecodeWalFileHeader(const uint8_t* data, size_t remaining,
                                    WalFileHeader& out, size_t& consumed);
WalDecodeStatus DecodeWalFrameHeader(const uint8_t* data, size_t remaining,
                                     WalFrameHeader& out, size_t& consumed);

/// Validates a fully read frame: recomputes the frame checksum over the
/// header bytes preceding the checksum field plus the payload. Returns kOk
/// on success.
WalDecodeStatus ValidateWalFrame(const WalFrameHeader& header,
                                 const uint8_t* frame_header_bytes,
                                 const uint8_t* payload);

/// Header bytes covered by frame_checksum: everything except the checksum
/// field itself.
constexpr size_t kWalFrameHeaderStableSize =
    kWalFrameHeaderSize - sizeof(uint32_t);

}  // namespace neug
