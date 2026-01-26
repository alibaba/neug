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

/**
 * This file is originally from the Kùzu project
 * (https://github.com/kuzudb/kuzu) Licensed under the MIT License. Modified by
 * Zhou Xiaoli in 2025 to support Neug-specific features.
 */

#pragma once

#include <cstdint>

namespace neug {

namespace testing {
class BaseGraphTest;
class PrivateGraphTest;
class TestHelper;
class TestRunner;
class TinySnbDDLTest;
class TinySnbCopyCSVTransactionTest;
}  // namespace testing

namespace benchmark {
class Benchmark;
}  // namespace benchmark

namespace binder {
class Expression;
class BoundStatementResult;
class PropertyExpression;
}  // namespace binder

namespace catalog {
class Catalog;
}  // namespace catalog

namespace common {
enum class StatementType : uint8_t;
class Value;
struct FileInfo;
class VirtualFileSystem;
}  // namespace common

namespace storage {
class MemoryManager;
class BufferManager;
class StatsManager;
class WAL;
enum class WALReplayMode : uint8_t;
}  // namespace storage

namespace planner {
class LogicalOperator;
class LogicalPlan;
}  // namespace planner

namespace processor {
class QueryProcessor;
class FactorizedTable;
class FlatTupleIterator;
class PhysicalOperator;
class PhysicalPlan;
}  // namespace processor

namespace transaction {
class Transaction;
class TransactionManager;
class TransactionContext;
}  // namespace transaction

}  // namespace neug
