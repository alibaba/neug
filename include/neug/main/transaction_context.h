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

#include <stdint.h>

#include <utility>
#include <variant>

#include <glog/logging.h>

#include "neug/transaction/current_cow_write_transaction.h"
#include "neug/transaction/read_transaction.h"
#include "neug/transaction/snapshot_cow_write_transaction.h"
#include "neug/utils/result.h"

namespace neug {

class ServiceTransactionManager;

enum class TransactionMode : uint8_t {
  kReadOnly,
  kReadWrite,
};

/**
 * @brief Session-owned explicit transaction state and concrete owner.
 *
 * This is deliberately a small tagged owner rather than a common transaction
 * interface. ExecutionSlot creates the concrete transaction; an embedded
 * Connection or service session owns this context across statements.
 */
class TransactionContext {
 public:
  enum class State : uint8_t {
    kIdle,
    kActive,
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
  bool PrivateViewChanged() const noexcept { return private_view_changed_; }

  void Begin(ReadTransaction transaction) {
    transaction_.emplace<ReadTransaction>(std::move(transaction));
    mode_ = TransactionMode::kReadOnly;
    state_ = State::kActive;
  }

  void Begin(CurrentCowWriteTransaction transaction) {
    transaction_.emplace<CurrentCowWriteTransaction>(std::move(transaction));
    mode_ = TransactionMode::kReadWrite;
    state_ = State::kActive;
  }

  void Begin(SnapshotCowWriteTransaction transaction) {
    transaction_.emplace<SnapshotCowWriteTransaction>(std::move(transaction));
    mode_ = TransactionMode::kReadWrite;
    state_ = State::kActive;
  }

  ReadTransaction& ReadTransactionOwner() {
    return std::get<ReadTransaction>(transaction_);
  }
  const ReadTransaction& ReadTransactionOwner() const {
    return std::get<ReadTransaction>(transaction_);
  }

  bool IsSnapshotWriteTransaction() const noexcept {
    return std::holds_alternative<SnapshotCowWriteTransaction>(transaction_);
  }

  template <typename Visitor>
  decltype(auto) VisitWriteTransaction(Visitor&& visitor) {
    if (auto* transaction =
            std::get_if<CurrentCowWriteTransaction>(&transaction_)) {
      return std::forward<Visitor>(visitor)(*transaction);
    }
    return std::forward<Visitor>(visitor)(
        std::get<SnapshotCowWriteTransaction>(transaction_));
  }

  template <typename Visitor>
  decltype(auto) VisitWriteTransaction(Visitor&& visitor) const {
    if (const auto* transaction =
            std::get_if<CurrentCowWriteTransaction>(&transaction_)) {
      return std::forward<Visitor>(visitor)(*transaction);
    }
    return std::forward<Visitor>(visitor)(
        std::get<SnapshotCowWriteTransaction>(transaction_));
  }

  void MarkPrivateViewChanged() noexcept { private_view_changed_ = true; }

  Status Commit() {
    if (IsReadOnly()) {
      if (!ReadTransactionOwner().Commit()) {
        AbortAndMarkRollbackOnly();
        return Status::InternalError("Read transaction commit failed.");
      }
      ResetToIdle();
      return Status::OK();
    }

    auto status = VisitWriteTransaction(
        [](auto& transaction) { return transaction.Commit(); });
    if (status.ok()) {
      ResetToIdle();
    } else {
      AbortAndMarkRollbackOnly();
    }
    return status;
  }

 private:
  friend class ServiceTransactionManager;

  Status PrepareCommit() {
    if (IsReadOnly()) {
      return Status::OK();
    }
    if (!IsSnapshotWriteTransaction()) {
      return Status(StatusCode::ERR_TX_STATE_CONFLICT,
                    "This transaction cannot use the service commit path.");
    }
    auto status =
        std::get<SnapshotCowWriteTransaction>(transaction_).PrepareCommit();
    if (!status.ok()) {
      AbortAndMarkRollbackOnly();
    }
    return status;
  }

  Status CommitPrepared() {
    if (IsReadOnly()) {
      if (!ReadTransactionOwner().Commit()) {
        AbortAndMarkRollbackOnly();
        return Status::InternalError("Read transaction commit failed.");
      }
      ResetToIdle();
      return Status::OK();
    }
    CHECK(IsSnapshotWriteTransaction());
    auto status =
        std::get<SnapshotCowWriteTransaction>(transaction_).CommitPrepared();
    if (status.ok()) {
      ResetToIdle();
    } else {
      AbortAndMarkRollbackOnly();
    }
    return status;
  }

 public:
  void Rollback() noexcept {
    if (IsActive()) {
      if (IsReadOnly()) {
        ReadTransactionOwner().Abort();
      } else {
        VisitWriteTransaction([](auto& transaction) { transaction.Abort(); });
      }
    }
    ResetToIdle();
  }

  void AbortAndMarkRollbackOnly() noexcept {
    if (IsActive()) {
      if (IsReadOnly()) {
        ReadTransactionOwner().Abort();
      } else {
        VisitWriteTransaction([](auto& transaction) { transaction.Abort(); });
      }
    }
    transaction_.emplace<std::monostate>();
    private_view_changed_ = false;
    state_ = State::kRollbackOnly;
  }

  const Schema& schema() const {
    CHECK(state_ != State::kIdle)
        << "TransactionContext::schema() requires an active transaction";
    if (IsReadOnly()) {
      return ReadTransactionOwner().schema();
    }
    return VisitWriteTransaction([](const auto& transaction) -> const Schema& {
      return transaction.schema();
    });
  }

 private:
  void ResetToIdle() noexcept {
    transaction_.emplace<std::monostate>();
    private_view_changed_ = false;
    state_ = State::kIdle;
  }

  State state_{State::kIdle};
  TransactionMode mode_{TransactionMode::kReadOnly};
  bool private_view_changed_{false};
  std::variant<std::monostate, ReadTransaction, CurrentCowWriteTransaction,
               SnapshotCowWriteTransaction>
      transaction_;
};

}  // namespace neug
