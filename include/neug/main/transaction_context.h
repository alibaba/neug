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
#include "neug/transaction/snapshot_cow_write_transaction.h"
#include "neug/transaction/snapshot_read_transaction.h"
#include "neug/utils/result.h"

namespace neug {

/** Access policy fixed when a Connection begins an explicit transaction. */
enum class TransactionMode : uint8_t {
  /** Pin a published read view and reject writes. */
  kReadOnly,
  /** Hold a private COW view and publish it only at Commit(). */
  kReadWrite,
};

/**
 * @brief Explicit transaction state and its concrete owner.
 *
 * ExecutionSlot constructs the concrete owner. A Connection or service session
 * keeps it across statements without introducing a common transaction
 * interface. A failed statement aborts the concrete owner and leaves this
 * context rollback-only; after that failure, only Rollback() returns it to
 * idle.
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

 private:
  template <typename Func>
  decltype(auto) VisitCowWriteOwner(Func&& func) {
    CHECK(IsActive() && !IsReadOnly());
    if (std::holds_alternative<CurrentCowWriteTransaction>(transaction_)) {
      return std::forward<Func>(func)(
          std::get<CurrentCowWriteTransaction>(transaction_));
    }
    return std::forward<Func>(func)(
        std::get<SnapshotCowWriteTransaction>(transaction_));
  }

  template <typename Func>
  decltype(auto) VisitCowWriteOwner(Func&& func) const {
    CHECK(IsActive() && !IsReadOnly());
    if (std::holds_alternative<CurrentCowWriteTransaction>(transaction_)) {
      return std::forward<Func>(func)(
          std::get<CurrentCowWriteTransaction>(transaction_));
    }
    return std::forward<Func>(func)(
        std::get<SnapshotCowWriteTransaction>(transaction_));
  }

 public:
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

 private:
  friend class ExecutionSlot;
  friend class ServiceTransactionManager;

  void Begin(SnapshotCowWriteTransaction transaction) {
    transaction_.emplace<SnapshotCowWriteTransaction>(std::move(transaction));
    mode_ = TransactionMode::kReadWrite;
    state_ = State::kActive;
  }

  SnapshotReadTransaction& ReadTransactionOwner() {
    return std::get<SnapshotReadTransaction>(transaction_);
  }
  const SnapshotReadTransaction& ReadTransactionOwner() const {
    return std::get<SnapshotReadTransaction>(transaction_);
  }

 public:
  Status Commit() {
    if (IsReadOnly()) {
      if (!ReadTransactionOwner().Commit()) {
        AbortAndMarkRollbackOnly();
        return Status::InternalError("Read transaction commit failed.");
      }
      ResetToIdle();
      return Status::OK();
    }

    CHECK(std::holds_alternative<CurrentCowWriteTransaction>(transaction_))
        << "TP snapshot writes must use the prepared commit path";
    auto status = std::get<CurrentCowWriteTransaction>(transaction_).Commit();
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
        VisitCowWriteOwner([](auto& transaction) { transaction.Abort(); });
      }
    }
    ResetToIdle();
  }

  void AbortAndMarkRollbackOnly() noexcept {
    if (IsActive()) {
      if (IsReadOnly()) {
        ReadTransactionOwner().Abort();
      } else {
        VisitCowWriteOwner([](auto& transaction) { transaction.Abort(); });
      }
    }
    transaction_.emplace<std::monostate>();
    state_ = State::kRollbackOnly;
  }

  const Schema& schema() const {
    CHECK(IsActive())
        << "TransactionContext::schema() requires an active transaction";
    if (IsReadOnly()) {
      return ReadTransactionOwner().schema();
    }
    return VisitCowWriteOwner([](const auto& transaction) -> const Schema& {
      return transaction.schema();
    });
  }

 private:
  Status PrepareTpSnapshotCommit() {
    CHECK(IsActive() && !IsReadOnly());
    if (!std::holds_alternative<SnapshotCowWriteTransaction>(transaction_)) {
      return Status::InternalError(
          "Only TP snapshot write transactions can be prepared.");
    }
    return std::get<SnapshotCowWriteTransaction>(transaction_).PrepareCommit();
  }

  Status CommitPreparedTpSnapshot() {
    CHECK(IsActive() && !IsReadOnly());
    if (!std::get<SnapshotCowWriteTransaction>(transaction_).CommitPrepared()) {
      AbortAndMarkRollbackOnly();
      return Status::InternalError("Prepared write transaction commit failed.");
    }
    ResetToIdle();
    return Status::OK();
  }

  void ResetToIdle() noexcept {
    transaction_.emplace<std::monostate>();
    state_ = State::kIdle;
  }

  State state_{State::kIdle};
  TransactionMode mode_{TransactionMode::kReadOnly};
  std::variant<std::monostate, SnapshotReadTransaction,
               CurrentCowWriteTransaction, SnapshotCowWriteTransaction>
      transaction_;
};

}  // namespace neug
