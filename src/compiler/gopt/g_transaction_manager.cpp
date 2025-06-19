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

#include "src/include/gopt/g_transaction_manager.h"

#include "src/include/gopt/g_constants.h"

namespace gs {
namespace transaction {
GTransactionManager::GTransactionManager(storage::WAL& wal)
    : TransactionManager(wal) {}

std::unique_ptr<Transaction> GTransactionManager::beginTransaction(
    main::ClientContext& clientContext, TransactionType type) {
  return std::make_unique<Transaction>(
      gs::transaction::TransactionType::DUMMY,
      gs::transaction::Transaction::DUMMY_TRANSACTION_ID,
      common::INVALID_TRANSACTION);
}

void GTransactionManager::commit(main::ClientContext& clientContext) {}
void rollback(main::ClientContext& clientContext, Transaction* transaction) {}

void GTransactionManager::checkpoint(main::ClientContext& clientContext) {}
void GTransactionManager::rollback(main::ClientContext& clientContext,
                                   Transaction* transaction) {}
}  // namespace transaction
}  // namespace gs