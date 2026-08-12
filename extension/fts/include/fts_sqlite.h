#pragma once

#include <cstdint>
#include <mutex>
#include <string>

struct sqlite3;
struct sqlite3_stmt;

namespace neug::fts_ext {

class SQLiteStatement {
 public:
  SQLiteStatement() = default;
  explicit SQLiteStatement(sqlite3_stmt* statement) : statement_(statement) {}
  ~SQLiteStatement();

  SQLiteStatement(const SQLiteStatement&) = delete;
  SQLiteStatement& operator=(const SQLiteStatement&) = delete;
  SQLiteStatement(SQLiteStatement&& other) noexcept;
  SQLiteStatement& operator=(SQLiteStatement&& other) noexcept;

  void BindText(int parameter, const std::string& value);
  void BindInt64(int parameter, int64_t value);
  void Reset();
  int Step();
  int64_t ColumnInt64(int column) const;
  double ColumnDouble(int column) const;
  sqlite3_stmt* get() const { return statement_; }
  std::mutex& mutex() { return mutex_; }

 private:
  std::mutex mutex_;
  sqlite3_stmt* statement_{nullptr};
};

class SQLiteConnection {
 public:
  SQLiteConnection() = default;
  ~SQLiteConnection();

  SQLiteConnection(const SQLiteConnection&) = delete;
  SQLiteConnection& operator=(const SQLiteConnection&) = delete;

  void Open(const std::string& path);
  void Close();
  void Execute(const std::string& sql);
  SQLiteStatement Prepare(const std::string& sql);
  void Flush();

  bool IsOpen() const { return connection_ != nullptr; }
  bool InTransaction() const;

 private:
  sqlite3* connection_{nullptr};
};

}  // namespace neug::fts_ext
