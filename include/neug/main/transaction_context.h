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

#include <cstdint>
#include <utility>
#include <variant>

#include <glog/logging.h>

#include "neug/transaction/current_cow_write_transaction.h"
#include "neug/transaction/snapshot_read_transaction.h"
#include "neug/utils/result.h"

namespace neug {

/** Access policy fixed when a Connection begins an explicit transaction. */
enum class TransactionMode : uint8_t {
  /** Pin a published read view and reject writes. */
  kReadOnly,
  /** Hold a private AP COW view and publish it only at Commit(). */
  kReadWrite,
};

/**
 * @brief Connection-owned explicit transaction state and concrete owner.
 *
 * ExecutionSlot constructs the concrete owner. Connection keeps it across
 * statements without introducing a common transaction interface. A failed
 * statement aborts the concrete owner and leaves this context rollback-only;
 * after that failure, only Rollback() returns it to idle.
 */
class TransactionContext {
 public:
  /** Explicit transaction lifecycle; rollback-only is still unfinished. */
  enum class State : uint8_t {
    /** No owner is present and auto-commit queries may execute. */
    kIdle,
    /** The owner is usable for transaction queries. */
    kActive,
    /** The owner was aborted after failure; only rollback may clear the state.
     */
    kRollbackOnly,
  };

  bool HasActiveTransaction() const noexcept { return state_ != State::kIdle; }
  bool IsActive() const noexcept { return state_ == State::kActive; }
  bool IsRollbackOnly() const noexcept {
    return state_ == State::kRollbackOnly;
  }
  bool IsReadOnly() const noexcept {
    return mode_ == TransactionMode::kReadOnly;
  }
  void Begin(SnapshotReadTransaction transaction) {
    transaction_.emplace<SnapshotReadTransaction>(std::move(transaction));
    mode_ = TransactionMode::kReadOnly;
    state_ = State::kActive;
  }

  void Begin(CurrentCowWriteTransaction transaction) {
    transaction_.emplace<CurrentCowWriteTransaction>(std::move(transaction));
    mode_ = TransactionMode::kReadWrite;
    state_ = State::kActive;
  }

  SnapshotReadTransaction& ReadTransactionOwner() {
    return std::get<SnapshotReadTransaction>(transaction_);
  }
  const SnapshotReadTransaction& ReadTransactionOwner() const {
    return std::get<SnapshotReadTransaction>(transaction_);
  }
  CurrentCowWriteTransaction& WriteTransactionOwner() {
    return std::get<CurrentCowWriteTransaction>(transaction_);
  }
  const CurrentCowWriteTransaction& WriteTransactionOwner() const {
    return std::get<CurrentCowWriteTransaction>(transaction_);
  }

  Status Commit() {
    if (IsReadOnly()) {
      if (!ReadTransactionOwner().Commit()) {
        AbortAndMarkRollbackOnly();
        return Status::InternalError("Read transaction commit failed.");
      }
      ResetToIdle();
      return Status::OK();
    }

    auto status = WriteTransactionOwner().Commit();
    if (status.ok()) {
      ResetToIdle();
    } else {
      AbortAndMarkRollbackOnly();
    }
    return status;
  }

  void Rollback() noexcept {
    if (IsActive()) {
      if (IsReadOnly()) {
        ReadTransactionOwner().Abort();
      } else {
        WriteTransactionOwner().Abort();
      }
    }
    ResetToIdle();
  }

  void AbortAndMarkRollbackOnly() noexcept {
    if (IsActive()) {
      if (IsReadOnly()) {
        ReadTransactionOwner().Abort();
      } else {
        WriteTransactionOwner().Abort();
      }
    }
    transaction_.emplace<std::monostate>();
    state_ = State::kRollbackOnly;
  }

  const Schema& schema() const {
    CHECK(IsActive())
        << "TransactionContext::schema() requires an active transaction";
    return IsReadOnly() ? ReadTransactionOwner().schema()
                        : WriteTransactionOwner().schema();
  }

 private:
  void ResetToIdle() noexcept {
    transaction_.emplace<std::monostate>();
    state_ = State::kIdle;
  }

  State state_{State::kIdle};
  TransactionMode mode_{TransactionMode::kReadOnly};
  std::variant<std::monostate, SnapshotReadTransaction,
               CurrentCowWriteTransaction>
      transaction_;
};

}  // namespace neug
