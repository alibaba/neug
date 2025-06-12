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
#include "storage/wal/wal.h"
#include "transaction/transaction_manager.h"

namespace gs {
namespace transaction {
class GTransactionManager : public TransactionManager {
 public:
  explicit GTransactionManager(storage::WAL& wal);
  std::unique_ptr<Transaction> beginTransaction(
      main::ClientContext& clientContext, TransactionType type) override;
  void commit(main::ClientContext& clientContext) override;
  void rollback(main::ClientContext& clientContext,
                Transaction* transaction) override;
  void checkpoint(main::ClientContext& clientContext) override;
};
}  // namespace transaction
}  // namespace gs