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

#include "fts_sqlite.h"

#include <sqlite3.h>

#include <utility>

#include "neug/utils/exception/exception.h"

namespace neug::fts_ext {
namespace {

std::string SQLiteError(sqlite3* connection, const std::string& context,
                        int code) {
  const char* message =
      connection ? sqlite3_errmsg(connection) : sqlite3_errstr(code);
  return context + ": " + (message ? message : "unknown SQLite error") +
         " (code " + std::to_string(code) + ")";
}

}  // namespace

SQLiteStatement::~SQLiteStatement() {
  if (statement_) {
    sqlite3_finalize(statement_);
  }
}

SQLiteStatement::SQLiteStatement(SQLiteStatement&& other) noexcept
    : statement_(std::exchange(other.statement_, nullptr)) {}

SQLiteStatement& SQLiteStatement::operator=(SQLiteStatement&& other) noexcept {
  if (this != &other) {
    if (statement_) {
      sqlite3_finalize(statement_);
    }
    statement_ = std::exchange(other.statement_, nullptr);
  }
  return *this;
}

void SQLiteStatement::BindText(int parameter, const std::string& value) {
  auto code =
      sqlite3_bind_text(statement_, parameter, value.data(),
                        static_cast<int>(value.size()), SQLITE_TRANSIENT);
  if (code != SQLITE_OK) {
    THROW_RUNTIME_ERROR(SQLiteError(sqlite3_db_handle(statement_),
                                    "SQLite bind text failed", code));
  }
}

void SQLiteStatement::BindInt64(int parameter, int64_t value) {
  auto code = sqlite3_bind_int64(statement_, parameter, value);
  if (code != SQLITE_OK) {
    THROW_RUNTIME_ERROR(SQLiteError(sqlite3_db_handle(statement_),
                                    "SQLite bind integer failed", code));
  }
}

void SQLiteStatement::Reset() {
  // sqlite3_reset() resets the statement even when it returns the error code
  // from the previous sqlite3_step() call.
  sqlite3_reset(statement_);
  auto code = sqlite3_clear_bindings(statement_);
  if (code != SQLITE_OK) {
    THROW_RUNTIME_ERROR(SQLiteError(sqlite3_db_handle(statement_),
                                    "SQLite clear bindings failed", code));
  }
}

int SQLiteStatement::Step() {
  auto code = sqlite3_step(statement_);
  if (code != SQLITE_ROW && code != SQLITE_DONE) {
    THROW_RUNTIME_ERROR(SQLiteError(sqlite3_db_handle(statement_),
                                    "SQLite statement execution failed", code));
  }
  return code;
}

int64_t SQLiteStatement::ColumnInt64(int column) const {
  return sqlite3_column_int64(statement_, column);
}

double SQLiteStatement::ColumnDouble(int column) const {
  return sqlite3_column_double(statement_, column);
}

SQLiteConnection::~SQLiteConnection() { Close(); }

void SQLiteConnection::Open(const std::string& path) {
  Close();
  auto code =
      sqlite3_open_v2(path.c_str(), &connection_,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, nullptr);
  if (code != SQLITE_OK) {
    auto error =
        SQLiteError(connection_, "SQLite connection open failed", code);
    Close();
    THROW_RUNTIME_ERROR(error);
  }
  sqlite3_extended_result_codes(connection_, 1);
  Execute("PRAGMA journal_mode=WAL;");
  Execute("PRAGMA synchronous=FULL;");
}

void SQLiteConnection::Close() {
  if (!connection_) {
    return;
  }
  auto* connection = std::exchange(connection_, nullptr);
  sqlite3_close(connection);
}

void SQLiteConnection::Execute(const std::string& sql) {
  char* raw_error = nullptr;
  auto code =
      sqlite3_exec(connection_, sql.c_str(), nullptr, nullptr, &raw_error);
  if (code != SQLITE_OK) {
    std::string error = raw_error ? raw_error : sqlite3_errmsg(connection_);
    sqlite3_free(raw_error);
    THROW_RUNTIME_ERROR("SQLite execute failed: " + error + " (code " +
                        std::to_string(code) + ")");
  }
}

SQLiteStatement SQLiteConnection::Prepare(const std::string& sql) {
  sqlite3_stmt* statement = nullptr;
  auto code =
      sqlite3_prepare_v2(connection_, sql.c_str(), -1, &statement, nullptr);
  if (code != SQLITE_OK) {
    THROW_RUNTIME_ERROR(
        SQLiteError(connection_, "SQLite statement prepare failed", code));
  }
  return SQLiteStatement(statement);
}

void SQLiteConnection::Flush() {
  auto code = sqlite3_db_cacheflush(connection_);
  if (code != SQLITE_OK) {
    THROW_RUNTIME_ERROR(
        SQLiteError(connection_, "SQLite database flush failed", code));
  }
}

bool SQLiteConnection::InTransaction() const {
  return connection_ && sqlite3_get_autocommit(connection_) == 0;
}

fts5_api* SQLiteConnection::GetFTS5API() const {
  sqlite3_stmt* statement = nullptr;
  auto code = sqlite3_prepare_v2(connection_, "SELECT fts5(?1)", -1, &statement,
                                 nullptr);
  if (code != SQLITE_OK) {
    THROW_RUNTIME_ERROR(
        SQLiteError(connection_, "Failed to prepare FTS5 API lookup", code));
  }

  fts5_api* api = nullptr;
  code = sqlite3_bind_pointer(statement, 1, &api, "fts5_api_ptr", nullptr);
  if (code == SQLITE_OK) {
    code = sqlite3_step(statement);
    if (code == SQLITE_ROW || code == SQLITE_DONE) {
      code = SQLITE_OK;
    }
  }
  const auto finalize_code = sqlite3_finalize(statement);
  if (code == SQLITE_OK && finalize_code != SQLITE_OK) {
    code = finalize_code;
  }
  if (code != SQLITE_OK || api == nullptr) {
    THROW_RUNTIME_ERROR(
        SQLiteError(connection_, "Failed to obtain FTS5 API", code));
  }
  return api;
}

}  // namespace neug::fts_ext
