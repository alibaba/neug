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

#include <sys/wait.h>
#include <unistd.h>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "neug/transaction/wal/local_wal_parser.h"
#include "neug/transaction/wal/local_wal_writer.h"
#include "neug/transaction/wal/wal.h"
#include "neug/utils/exception/exception.h"

namespace neug {
namespace {

// ---------------------------------------------------------------------------
// Codec table-driven tests
// ---------------------------------------------------------------------------

struct WalFileFixture {
  WalFileHeader file_header;
  std::array<uint8_t, kWalFileHeaderSize> file_header_bytes;
};

WalFileFixture MakeFileHeader() {
  WalFileFixture fixture;
  fixture.file_header_bytes = EncodeWalFileHeader(fixture.file_header);
  return fixture;
}

/// Encodes one committed frame (header + payload + trailer) into bytes.
std::vector<uint8_t> EncodeFrame(uint32_t ts, WalRecordKind kind,
                                 const std::vector<uint8_t>& payload) {
  WalFrameHeader header;
  header.record_kind = kind;
  header.commit_timestamp = ts;
  header.payload_length = payload.size();
  auto header_bytes = EncodeWalFrameHeader(header);

  WalFrameTrailer trailer;
  auto trailer_bytes = EncodeWalFrameTrailer(trailer, header_bytes.data(),
                                             payload.data(), payload.size());

  std::vector<uint8_t> bytes;
  bytes.insert(bytes.end(), header_bytes.begin(), header_bytes.end());
  bytes.insert(bytes.end(), payload.begin(), payload.end());
  bytes.insert(bytes.end(), trailer_bytes.begin(), trailer_bytes.end());
  return bytes;
}

TEST(WalCodecTest, FrameRoundTripAllKinds) {
  const std::vector<uint8_t> payload = {0xDE, 0xAD, 0xBE, 0xEF};
  const std::vector<WalRecordKind> kinds = {WalRecordKind::kInsert,
                                            WalRecordKind::kCowUpdate,
                                            WalRecordKind::kCompact};
  for (const auto kind : kinds) {
    // kCompact frames carry no payload by protocol; encode empty for it.
    const auto& body =
        kind == WalRecordKind::kCompact ? std::vector<uint8_t>{} : payload;
    const auto frame = EncodeFrame(/*ts=*/42, kind, body);

    WalFrameHeader header;
    size_t consumed = 0;
    ASSERT_EQ(
        DecodeWalFrameHeader(frame.data(), frame.size(), header, consumed),
        WalDecodeStatus::kOk)
        << "kind=" << WalRecordKindName(kind);
    EXPECT_EQ(consumed, kWalFrameHeaderSize);
    EXPECT_EQ(header.record_kind, kind);
    EXPECT_EQ(header.commit_timestamp, 42u);
    EXPECT_EQ(header.payload_length, body.size());

    const uint8_t* trailer_bytes =
        frame.data() + kWalFrameHeaderSize + body.size();
    WalFrameTrailer trailer;
    ASSERT_EQ(DecodeWalFrameTrailer(trailer_bytes, kWalFrameTrailerSize,
                                    trailer, consumed),
              WalDecodeStatus::kOk);
    EXPECT_EQ(consumed, kWalFrameTrailerSize);
    EXPECT_EQ(trailer.commit_marker, kWalCommitMarker);

    EXPECT_EQ(ValidateWalFrame(header, trailer, frame.data(),
                               frame.data() + kWalFrameHeaderSize),
              WalDecodeStatus::kOk);
  }
}

TEST(WalCodecTest, FileHeaderRoundTrip) {
  WalFileHeader header;
  header.writer_slot_id = 3;
  auto bytes = EncodeWalFileHeader(header);

  WalFileHeader decoded;
  size_t consumed = 0;
  ASSERT_EQ(DecodeWalFileHeader(bytes.data(), bytes.size(), decoded, consumed),
            WalDecodeStatus::kOk);
  EXPECT_EQ(consumed, kWalFileHeaderSize);
  EXPECT_EQ(decoded.writer_slot_id, 3u);
  EXPECT_EQ(decoded.reserved, 0u);
}

TEST(WalCodecTest, FileHeaderDecodeRejectsBadMagicVersionSizeTruncation) {
  WalFileHeader header;
  WalFileHeader decoded;
  size_t consumed = 0;

  auto bytes = EncodeWalFileHeader(header);
  EXPECT_EQ(DecodeWalFileHeader(bytes.data(), kWalFileHeaderSize - 1, decoded,
                                consumed),
            WalDecodeStatus::kTruncated);

  header.magic = 0x12345678;
  bytes = EncodeWalFileHeader(header);
  EXPECT_EQ(DecodeWalFileHeader(bytes.data(), bytes.size(), decoded, consumed),
            WalDecodeStatus::kBadMagic);

  header = WalFileHeader{};
  header.format_version = 2;
  bytes = EncodeWalFileHeader(header);
  EXPECT_EQ(DecodeWalFileHeader(bytes.data(), bytes.size(), decoded, consumed),
            WalDecodeStatus::kBadVersion);

  header = WalFileHeader{};
  header.header_size = kWalFileHeaderSize + 4;
  bytes = EncodeWalFileHeader(header);
  EXPECT_EQ(DecodeWalFileHeader(bytes.data(), bytes.size(), decoded, consumed),
            WalDecodeStatus::kBadHeaderSize);
}

TEST(WalCodecTest, FrameHeaderDecodeRejectsUnknownKindSizeOverflow) {
  const std::vector<uint8_t> payload = {1, 2, 3};
  auto frame = EncodeFrame(1, WalRecordKind::kInsert, payload);
  WalFrameHeader decoded;
  size_t consumed = 0;

  EXPECT_EQ(DecodeWalFrameHeader(frame.data(), kWalFrameHeaderSize - 1, decoded,
                                 consumed),
            WalDecodeStatus::kTruncated);

  // Unknown record kind: patch the kind field; the kind check runs before
  // checksum validation at decode time.
  auto corrupted = frame;
  corrupted[4] = 9;
  EXPECT_EQ(DecodeWalFrameHeader(corrupted.data(), corrupted.size(), decoded,
                                 consumed),
            WalDecodeStatus::kUnknownRecordKind);

  // Bad header size.
  corrupted = frame;
  corrupted[8] = kWalFrameHeaderSize + 1;
  EXPECT_EQ(DecodeWalFrameHeader(corrupted.data(), corrupted.size(), decoded,
                                 consumed),
            WalDecodeStatus::kBadHeaderSize);

  // payload_length overflow beyond kWalMaxPayloadLength.
  corrupted = frame;
  corrupted[16] = 0xFF;
  corrupted[17] = 0xFF;
  corrupted[18] = 0xFF;
  corrupted[19] = 0xFF;
  corrupted[20] = 0xFF;
  EXPECT_EQ(DecodeWalFrameHeader(corrupted.data(), corrupted.size(), decoded,
                                 consumed),
            WalDecodeStatus::kPayloadTooLarge);
}

TEST(WalCodecTest, TamperingDetectedInPayloadHeaderAndMarker) {
  const std::vector<uint8_t> payload = {0x11, 0x22, 0x33, 0x44};
  auto frame = EncodeFrame(7, WalRecordKind::kCowUpdate, payload);

  WalFrameHeader header;
  size_t consumed = 0;
  ASSERT_EQ(DecodeWalFrameHeader(frame.data(), frame.size(), header, consumed),
            WalDecodeStatus::kOk);
  WalFrameTrailer trailer;
  ASSERT_EQ(
      DecodeWalFrameTrailer(frame.data() + kWalFrameHeaderSize + payload.size(),
                            kWalFrameTrailerSize, trailer, consumed),
      WalDecodeStatus::kOk);

  // Payload bit flip: the frame checksum must catch it.
  auto flipped = frame;
  flipped[kWalFrameHeaderSize + 1] ^= 0x80;
  WalFrameHeader flipped_header;
  ASSERT_EQ(DecodeWalFrameHeader(flipped.data(), flipped.size(), flipped_header,
                                 consumed),
            WalDecodeStatus::kOk);
  EXPECT_EQ(ValidateWalFrame(flipped_header, trailer, flipped.data(),
                             flipped.data() + kWalFrameHeaderSize),
            WalDecodeStatus::kBadChecksum);

  // Frame header bit flip: the frame checksum must catch it too.
  auto flipped_header_bytes = frame;
  flipped_header_bytes[12] ^= 0x01;  // commit_timestamp field
  WalFrameHeader ts_header;
  ASSERT_EQ(
      DecodeWalFrameHeader(flipped_header_bytes.data(),
                           flipped_header_bytes.size(), ts_header, consumed),
      WalDecodeStatus::kOk);
  EXPECT_EQ(ValidateWalFrame(ts_header, trailer, flipped_header_bytes.data(),
                             flipped_header_bytes.data() + kWalFrameHeaderSize),
            WalDecodeStatus::kBadChecksum);

  // Trailer marker tamper: caught at decode time by the marker check, so a
  // corrupted commit marker never reaches validation.
  auto bad_marker = frame;
  bad_marker[kWalFrameHeaderSize + payload.size()] ^= 0xFF;
  WalFrameTrailer marker_trailer;
  EXPECT_EQ(DecodeWalFrameTrailer(
                bad_marker.data() + kWalFrameHeaderSize + payload.size(),
                kWalFrameTrailerSize, marker_trailer, consumed),
            WalDecodeStatus::kBadMagic);
}

// ---------------------------------------------------------------------------
// Parser file-level tests
// ---------------------------------------------------------------------------

void ExpectWalRecoveryError(const std::string& wal_dir,
                            const std::string& needle) {
  try {
    LocalWalParser parser(wal_dir);
    FAIL() << "Expected WalRecoveryException containing '" << needle << "'";
  } catch (const neug::exception::WalRecoveryException& e) {
    EXPECT_NE(std::string(e.what()).find(needle), std::string::npos)
        << "actual: " << e.what();
  }
}

/// Shared temp-dir lifecycle: each test gets an isolated wal directory
/// under the system temp dir, removed on teardown.
class WalTempDirTest : public ::testing::Test {
 protected:
  explicit WalTempDirTest(const char* prefix) : prefix_(prefix) {}

  void SetUp() override {
    const auto* info = ::testing::UnitTest::GetInstance()->current_test_info();
    wal_dir_ = (std::filesystem::temp_directory_path() /
                (prefix_ + std::to_string(::getpid()) + "_" + info->name()))
                   .string();
    std::filesystem::remove_all(wal_dir_);
  }

  void TearDown() override { std::filesystem::remove_all(wal_dir_); }

  void ExpectRecoveryErrorContaining(const std::string& needle) {
    ExpectWalRecoveryError(wal_dir_, needle);
  }

  std::string wal_dir_;

 private:
  std::string prefix_;
};

class WalParserFileTest : public WalTempDirTest {
 protected:
  WalParserFileTest() : WalTempDirTest("neug_wal_codec_test_") {}

  void SetUp() override {
    WalTempDirTest::SetUp();
    std::filesystem::create_directories(wal_dir_);
  }

  /// Clears the wal dir for multi-case tests that run several scenarios
  /// inside one TEST_F.
  void ResetDir() {
    std::filesystem::remove_all(wal_dir_);
    std::filesystem::create_directories(wal_dir_);
  }

  void WriteFile(const std::string& name, const std::vector<uint8_t>& bytes) {
    std::ofstream out(wal_dir_ + "/" + name, std::ios::binary);
    out.write(reinterpret_cast<const char*>(bytes.data()),
              static_cast<std::streamsize>(bytes.size()));
  }

  /// Builds a complete v1 file: file header + all encoded frames.
  std::vector<uint8_t> BuildWalFile(
      const std::vector<std::vector<uint8_t>>& frames) {
    auto fixture = MakeFileHeader();
    std::vector<uint8_t> bytes(fixture.file_header_bytes.begin(),
                               fixture.file_header_bytes.end());
    for (const auto& frame : frames) {
      bytes.insert(bytes.end(), frame.begin(), frame.end());
    }
    return bytes;
  }
};

TEST_F(WalParserFileTest, ParsesCommittedFrames) {
  const std::vector<uint8_t> payload = {1, 2, 3, 4, 5};
  WriteFile("thread_0_0.wal",
            BuildWalFile({EncodeFrame(10, WalRecordKind::kInsert, payload),
                          EncodeFrame(11, WalRecordKind::kCompact, {})}));

  LocalWalParser parser(wal_dir_);
  EXPECT_EQ(parser.last_ts(), 11u);
  ASSERT_EQ(parser.replay_units().size(), 2u);
  EXPECT_EQ(parser.replay_units()[0].kind, WalRecordKind::kInsert);
  EXPECT_EQ(parser.replay_units()[0].commit_timestamp, 10u);
  EXPECT_EQ(parser.replay_units()[0].payload,
            std::string(payload.begin(), payload.end()));
  EXPECT_EQ(parser.replay_units()[1].kind, WalRecordKind::kCompact);
  EXPECT_TRUE(parser.replay_units()[1].payload.empty());
}

TEST_F(WalParserFileTest, NonV1FileIsRejectedWithoutFallback) {
  // Pre-v1 content: a legacy redo record starting with a record kind, and a
  // file too short to hold a v1 header. Neither may be "best-effort" parsed.
  WriteFile("legacy.wal",
            std::vector<uint8_t>{0x01, 0x00, 0x00, 0x00, 0xAA, 0xBB});
  ExpectRecoveryErrorContaining("[unsupported_format]");

  ResetDir();
  WriteFile("short.wal", std::vector<uint8_t>(16, 0x00));
  ExpectRecoveryErrorContaining("[unsupported_format]");
}

TEST_F(WalParserFileTest, BadVersionIsRejected) {
  WalFileHeader header;
  header.format_version = 2;
  const auto encoded = EncodeWalFileHeader(header);
  WriteFile("thread_0_0.wal",
            std::vector<uint8_t>(encoded.begin(), encoded.end()));
  ExpectRecoveryErrorContaining("[unsupported_format]");
}

TEST_F(WalParserFileTest, NonTailPayloadCorruptionIsRejected) {
  // Two committed frames; flip a payload bit in the FIRST one. Corruption
  // before the final frame must never be treated as crash residue.
  auto frame1 =
      EncodeFrame(1, WalRecordKind::kInsert, std::vector<uint8_t>{1, 2, 3, 4});
  auto frame2 =
      EncodeFrame(2, WalRecordKind::kInsert, std::vector<uint8_t>{5, 6, 7, 8});
  frame1[kWalFrameHeaderSize + 1] ^= 0x40;
  WriteFile("thread_0_0.wal", BuildWalFile({frame1, frame2}));
  ExpectRecoveryErrorContaining("[corrupted_frame]");
}

TEST_F(WalParserFileTest, UnknownRecordKindIsRejected) {
  auto frame =
      EncodeFrame(1, WalRecordKind::kInsert, std::vector<uint8_t>{1, 2, 3});
  frame[4] = 42;  // unknown kind
  WriteFile("thread_0_0.wal", BuildWalFile({frame}));
  ExpectRecoveryErrorContaining("[unknown_record_kind]");
}

TEST_F(WalParserFileTest, KindPayloadConstraintsAreRejected) {
  // kCompact with a non-empty payload.
  WriteFile("thread_0_0.wal",
            BuildWalFile({EncodeFrame(1, WalRecordKind::kCompact,
                                      std::vector<uint8_t>{9})}));
  ExpectRecoveryErrorContaining("[corrupted_frame]");

  ResetDir();

  // kInsert with an empty payload.
  WriteFile("thread_0_0.wal",
            BuildWalFile({EncodeFrame(1, WalRecordKind::kInsert, {})}));
  ExpectRecoveryErrorContaining("[corrupted_frame]");
}

TEST_F(WalParserFileTest, TailResidueIsIgnored) {
  const std::vector<uint8_t> payload = {1, 2, 3, 4, 5, 6, 7, 8};

  // Each file carries one committed frame plus a torn tail of the next one.
  const auto build_torn = [&](uint32_t committed_ts, size_t torn_prefix) {
    const auto good =
        EncodeFrame(committed_ts, WalRecordKind::kInsert, payload);
    const auto next =
        EncodeFrame(committed_ts + 1, WalRecordKind::kInsert, payload);
    auto bytes = BuildWalFile({good});
    bytes.insert(bytes.end(), next.begin(), next.begin() + torn_prefix);
    return bytes;
  };

  // Torn frame header at EOF.
  WriteFile("a_half_header.wal", build_torn(1, kWalFrameHeaderSize / 2));
  // Torn payload at EOF.
  WriteFile("b_half_payload.wal",
            build_torn(2, kWalFrameHeaderSize + payload.size() / 2));
  // Torn trailer at EOF (payload complete, marker never written).
  WriteFile("c_half_trailer.wal",
            build_torn(3, kWalFrameHeaderSize + payload.size() +
                              kWalFrameTrailerSize / 2));

  LocalWalParser parser(wal_dir_);
  EXPECT_EQ(parser.last_ts(), 3u);
  ASSERT_EQ(parser.replay_units().size(), 3u);
  for (size_t i = 0; i < parser.replay_units().size(); ++i) {
    EXPECT_EQ(parser.replay_units()[i].commit_timestamp, i + 1);
  }
}

// ---------------------------------------------------------------------------
// Multi-writer merge tests
// ---------------------------------------------------------------------------

TEST_F(WalParserFileTest, InterleavedWritersMergeByTimestampWithGaps) {
  // Writer A: ts 2, 7; writer B: ts 3, 10. Gaps are allowed.
  WriteFile(
      "thread_0_0.wal",
      BuildWalFile(
          {EncodeFrame(2, WalRecordKind::kInsert, std::vector<uint8_t>{'a'}),
           EncodeFrame(7, WalRecordKind::kInsert, std::vector<uint8_t>{'c'})}));
  WriteFile("thread_0_1.wal",
            BuildWalFile({EncodeFrame(3, WalRecordKind::kCowUpdate,
                                      std::vector<uint8_t>{'b'}),
                          EncodeFrame(10, WalRecordKind::kCompact, {})}));

  LocalWalParser parser(wal_dir_);
  ASSERT_EQ(parser.replay_units().size(), 4u);
  EXPECT_EQ(parser.replay_units()[0].commit_timestamp, 2u);
  EXPECT_EQ(parser.replay_units()[1].commit_timestamp, 3u);
  EXPECT_EQ(parser.replay_units()[1].kind, WalRecordKind::kCowUpdate);
  EXPECT_EQ(parser.replay_units()[2].commit_timestamp, 7u);
  EXPECT_EQ(parser.replay_units()[3].commit_timestamp, 10u);
  EXPECT_EQ(parser.replay_units()[3].kind, WalRecordKind::kCompact);
  EXPECT_EQ(parser.last_ts(), 10u);
}

TEST_F(WalParserFileTest, DuplicateTimestampsAreAlwaysRejected) {
  struct Case {
    WalRecordKind first;
    WalRecordKind second;
  };
  const Case cases[] = {
      {WalRecordKind::kInsert, WalRecordKind::kInsert},
      {WalRecordKind::kCowUpdate, WalRecordKind::kCowUpdate},
      {WalRecordKind::kInsert, WalRecordKind::kCowUpdate},
  };
  int case_index = 0;
  for (const auto& c : cases) {
    ++case_index;
    ResetDir();
    WriteFile(
        "thread_0_0.wal",
        BuildWalFile({EncodeFrame(5, c.first, std::vector<uint8_t>{'x'})}));
    WriteFile(
        "thread_0_1.wal",
        BuildWalFile({EncodeFrame(5, c.second, std::vector<uint8_t>{'y'})}));
    try {
      LocalWalParser parser(wal_dir_);
      FAIL() << "case " << case_index << ": duplicate timestamp accepted";
    } catch (const neug::exception::WalRecoveryException& e) {
      EXPECT_NE(std::string(e.what()).find("[duplicate_timestamp]"),
                std::string::npos)
          << "case " << case_index << ": " << e.what();
      EXPECT_NE(std::string(e.what()).find("thread_0_0.wal"), std::string::npos)
          << "case " << case_index << ": both sources must be reported";
      EXPECT_NE(std::string(e.what()).find("thread_0_1.wal"), std::string::npos)
          << "case " << case_index << ": both sources must be reported";
    }
  }
}

// A failed frame attempt must be rolled back to the clean logical EOF; the
// writer stays usable and its next frame lands directly after the last
// committed one, so recovery never sees residue buried mid-file.
TEST_F(WalParserFileTest, FailedFrameAttemptsRollBackToCleanEof) {
  const auto phases = {LocalWalWriter::FailNextWrite::kHeader,
                       LocalWalWriter::FailNextWrite::kPayload,
                       LocalWalWriter::FailNextWrite::kTrailer};
  int phase_index = 0;
  for (const auto phase : phases) {
    ++phase_index;
    ResetDir();

    LocalWalWriter writer(wal_dir_, /*slot_id=*/0);
    writer.open(wal_dir_);
    ASSERT_TRUE(writer.append_frame(1, WalRecordKind::kInsert, "one", 3))
        << "phase_index=" << phase_index;

    writer.fail_next_write(phase);
    ASSERT_FALSE(writer.append_frame(2, WalRecordKind::kInsert, "two", 3))
        << "phase_index=" << phase_index;
    EXPECT_EQ(writer.write_phase(), WalWritePhase::kIdle)
        << "a rolled-back frame leaves no write in progress, phase_index="
        << phase_index;

    ASSERT_TRUE(writer.append_frame(3, WalRecordKind::kInsert, "three", 5))
        << "phase_index=" << phase_index;
    writer.close();

    LocalWalParser parser(wal_dir_);
    ASSERT_EQ(parser.replay_units().size(), 2u)
        << "phase_index=" << phase_index;
    EXPECT_EQ(parser.replay_units()[0].commit_timestamp, 1u);
    EXPECT_EQ(parser.replay_units()[0].payload, "one");
    EXPECT_EQ(parser.replay_units()[1].commit_timestamp, 3u);
    EXPECT_EQ(parser.replay_units()[1].payload, "three");
  }
}

// ---------------------------------------------------------------------------
// Subprocess crash tests: the child writes through the real LocalWalWriter
// and dies at a precise phase; the parent recovers.
// ---------------------------------------------------------------------------

class WalCrashSubprocessTest : public WalTempDirTest {
 protected:
  WalCrashSubprocessTest() : WalTempDirTest("neug_wal_crash_test_") {}

  /// Runs @p child_body in a forked process that exits via _exit() without
  /// any cleanup, simulating a crash. Waits for normal termination.
  void RunCrashChild(const std::function<void()>& child_body) {
    const pid_t pid = ::fork();
    ASSERT_NE(pid, -1);
    if (pid == 0) {
      child_body();
      ::_exit(0);
    }
    int status = 0;
    ASSERT_EQ(::waitpid(pid, &status, 0), pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);
  }
};

TEST_F(WalCrashSubprocessTest, CrashAtEachWritePhaseLeavesOnlyCommittedFrames) {
  const std::string payload_a = "committed-payload";
  const std::string payload_b = "crashed-payload-not-visible";

  const auto phases = {LocalWalWriter::FailNextWrite::kHeader,
                       LocalWalWriter::FailNextWrite::kPayload,
                       LocalWalWriter::FailNextWrite::kTrailer};
  int phase_index = 0;
  for (const auto phase : phases) {
    ++phase_index;
    std::filesystem::remove_all(wal_dir_);

    RunCrashChild([&] {
      LocalWalWriter writer(wal_dir_, /*slot_id=*/0);
      writer.open(wal_dir_);
      if (!writer.append_frame(1, WalRecordKind::kInsert, payload_a.data(),
                               payload_a.size())) {
        ::_exit(1);
      }
      writer.fail_next_write(phase);
      // The injected failure makes this commit fail cleanly; the process
      // then dies without closing the file, like a real crash.
      if (writer.append_frame(2, WalRecordKind::kInsert, payload_b.data(),
                              payload_b.size())) {
        ::_exit(1);
      }
    });

    LocalWalParser parser(wal_dir_);
    EXPECT_EQ(parser.last_ts(), 1u) << "phase_index=" << phase_index;
    ASSERT_EQ(parser.replay_units().size(), 1u)
        << "phase_index=" << phase_index;
    EXPECT_EQ(parser.replay_units()[0].payload, payload_a);
  }
}

TEST_F(WalCrashSubprocessTest, ReplayAcrossCompactInOrder) {
  RunCrashChild([&] {
    LocalWalWriter writer(wal_dir_, /*slot_id=*/0);
    writer.open(wal_dir_);
    if (!writer.append_frame(1, WalRecordKind::kInsert, "insert-a", 8) ||
        !writer.append_frame(2, WalRecordKind::kCompact, nullptr, 0) ||
        !writer.append_frame(3, WalRecordKind::kInsert, "insert-b", 8)) {
      ::_exit(1);
    }
  });

  LocalWalParser parser(wal_dir_);
  ASSERT_EQ(parser.replay_units().size(), 3u);
  EXPECT_EQ(parser.replay_units()[0].kind, WalRecordKind::kInsert);
  EXPECT_EQ(parser.replay_units()[1].kind, WalRecordKind::kCompact);
  EXPECT_EQ(parser.replay_units()[2].kind, WalRecordKind::kInsert);
  EXPECT_EQ(parser.replay_units()[2].payload, "insert-b");
}

TEST_F(WalCrashSubprocessTest, BitFlippedPayloadIsRejected) {
  RunCrashChild([&] {
    LocalWalWriter writer(wal_dir_, /*slot_id=*/0);
    writer.open(wal_dir_);
    if (!writer.append_frame(1, WalRecordKind::kInsert, "stable-payload", 14)) {
      ::_exit(1);
    }
  });

  // Corrupt one payload byte after the "crash".
  for (auto& entry : std::filesystem::directory_iterator(wal_dir_)) {
    std::fstream file(entry.path(),
                      std::ios::in | std::ios::out | std::ios::binary);
    ASSERT_TRUE(file.is_open());
    char byte = 0;
    file.seekp(kWalFileHeaderSize + kWalFrameHeaderSize + 2);
    file.read(&byte, 1);
    file.seekp(kWalFileHeaderSize + kWalFrameHeaderSize + 2);
    byte ^= 0x55;
    file.write(&byte, 1);
  }

  ExpectRecoveryErrorContaining("[corrupted_frame]");
}

}  // namespace
}  // namespace neug
