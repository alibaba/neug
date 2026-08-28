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

#include <yaml-cpp/yaml.h>
#include <cctype>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "neug/compiler/planner/gopt_planner.h"
#include "neug/compiler/planner/graph_planner.h"
#include "neug/config.h"
#include "neug/main/neug_db.h"
#include "neug/server/tp_execution_slot_pool.h"
#include "neug/transaction/in_place_compaction_transaction.h"
#include "neug/transaction/mvcc_insert_transaction.h"
#include "neug/transaction/snapshot_read_transaction.h"
#include "neug/utils/result.h"
#include "neug/utils/service_manager.h"
#include "neug/utils/service_utils.h"

namespace neug {

class ServiceTransactionManager;

/**
 * @brief NeuG database HTTP service for high-throughput scenarios.
 *
 * NeugDBService provides an HTTP interface layer for the NeuG graph database,
 * enabling remote query execution over HTTP. It manages the lifecycle of a
 * BRPC-based HTTP server that handles Cypher queries, service status requests,
 * and schema queries through RESTful endpoints.
 *
 * This is the C++ equivalent of Python's `Database.serve()` functionality,
 * designed for high-throughput Transaction Processing (TP) scenarios where
 * multiple clients need concurrent access to the database.
 *
 * **Usage Example:**
 * @code{.cpp}
 * #include <neug/main/neug_db.h>
 * #include <neug/server/neug_db_service.h>
 *
 * int main() {
 *   // 1. Open the database
 *   neug::NeugDB db;
 *   db.Open("/path/to/graph", 8);  // 8 threads
 *
 *   // 2. Create and configure service
 *   neug::ServiceConfig config;
 *   config.query_port = 10000;
 *   config.host_str = "0.0.0.0";
 *
 *   // 3. Start HTTP service
 *   neug::NeugDBService service(db, config);
 *   std::string url = service.Start();
 *   std::cout << "Service running at: " << url << std::endl;
 *
 *   // 4. Block until shutdown signal (Ctrl+C)
 *   service.run_and_wait_for_exit();
 *
 *   // 5. Cleanup
 *   db.Close();
 *   return 0;
 * }
 * @endcode
 *
 * **HTTP Endpoints:**
 * - `POST /cypher` - Execute Cypher queries
 * - `GET /schema` - Retrieve graph schema
 * - `GET /status` - Check service status
 * - `POST /transactions` - Begin an explicit TP transaction session
 * - `POST /transactions/{id}/query|commit|rollback` - Operate on a session
 *
 * **Thread Safety:** All public methods are thread-safe. The service uses
 * a TpExecutionSlotPool internally to handle concurrent requests efficiently.
 *
 * @see ExecutionSlot for execution slot-based query execution
 * @see TpExecutionSlotPool for execution slot management
 * @since v0.1.0
 */
class NeugDBService {
 public:
  /**
   * @brief Constructs a service around an existing database instance
   *
   * @param db Reference to the NeuG database that will handle queries
   *
   * @note The database should be opened and ready before creating the service
   * @note At most one NeugDBService can be associated with a NeugDB instance
   * at any given time. The association is released when the service is
   * destructed.
   * @warning Construction requires all existing embedded connections to be
   * closed first.
   *
   * @throws neug::exception::RuntimeError If local connections are still open
   * or another NeugDBService is already associated with the database
   */
  NeugDBService(neug::NeugDB& db,
                const ServiceConfig& config = ServiceConfig());

  /**
   * @brief Gets direct access to the underlying graph database
   *
   * @return Reference to the wrapped NeugDB instance
   *
   * @warning Direct database access bypasses the service layer
   */
  neug::NeugDB& db() { return db_; }

  /**
   * @brief Destructor that ensures proper cleanup
   *
   * Automatically stops the HTTP handler manager if it's running and
   * releases all associated resources.
   *
   * @warning All ExecutionSlotLease objects acquired from this service must be
   * destroyed before the service is destroyed.
   */
  ~NeugDBService();

  /**
   * @brief Starts the HTTP server
   *
   * Binds to the configured host and port and begins accepting HTTP requests.
   * Returns the full URL where the service is accessible.
   *
   * @return URL string in format "http://host:port" where service is running
   *
   * @throws std::runtime_error If service is not initialized
   * @throws std::runtime_error If service is already running
   * @throws std::runtime_error If unable to bind to configured address
   */
  std::string Start();

  /**
   * @brief Stops the HTTP server gracefully
   *
   * Stops accepting new connections and shuts down the BRPC server.
   * This method is thread-safe and can be called from signal handlers.
   *
   * @note Prints status messages to stderr if service is not properly
   * initialized
   * @note Protected by mutex to ensure thread-safe shutdown
   */
  void Stop();

  /**
   * @brief Retrieves the current service configuration
   *
   * @return Const reference to the ServiceConfig used during initialization
   *
   * @note Returns the configuration passed to init(), not runtime settings
   */
  const ServiceConfig& GetServiceConfig() const;

  /**
   * @brief Leases an execution slot from the internal TP pool.
   *
   * Returns an ExecutionSlotLease that automatically releases the execution
   * slot back to the pool when it goes out of scope.
   *
   * **Usage Example:**
   * @code{.cpp}
   * neug::NeugDBService service(db, config);
   * service.Start();
   *
   * // Lease an execution slot and execute a query.
   * auto lease = service.AcquireExecutionSlot();
   * auto result = lease->ExecuteTransactionalRequest(
   *     R"({"query": "MATCH (n) RETURN count(n)"})");
   *
   * // The ExecutionSlot is automatically returned when lease leaves scope.
   * @endcode
   *
   * @return ExecutionSlotLease managing the acquired execution slot
   * @note Blocks if no execution slot is available in the pool
   */
  neug::ExecutionSlotLease AcquireExecutionSlot();

  /**
   * @brief Checks if the HTTP server is currently running
   *
   * @return true if the underlying BRPC server is accepting connections
   *
   * @note This delegates to the HTTP handler manager's IsRunning() method
   * @note Thread-safe query of server state
   */
  bool IsRunning() const;

  /**
   * @brief Gets current service status information
   *
   * Returns status messages indicating the current state:
   * - "NeugDB service has not been inited!" if not initialized
   * - "NeugDB service has not been started!" if initialized but not running
   * - "NeugDB service is running ..." if actively serving requests
   *
   * @return Result containing status message with OK status code
   *
   * @note Always returns OK status, actual state is in the message string
   */
  neug::result<std::string> service_status();

  /**
   * @brief Starts service and blocks until shutdown signal
   *
   * Convenience method that starts the HTTP server and blocks the calling
   * thread until the server is asked to quit (via Stop() or signal).
   * Uses the underlying BRPC server's RunUntilAskedToQuit() mechanism.
   *
   * @throws std::runtime_error If service is not initialized
   * @throws std::runtime_error If service is already running
   * @throws std::runtime_error If HTTP handler manager is not available
   *
   * @note This is the typical way to run the service in production
   */
  void run_and_wait_for_exit();

  size_t getExecutedQueryNum() const;

  size_t ExecutionSlotNum() const {
    return execution_slot_pool_->ExecutionSlotNum();
  }

 private:
  NeugDBService() = delete;
  void startCompactThread();
  void stopCompactThread();
  void installBthreadRuntimeWait();
  void restoreNativeRuntimeWait() noexcept;

  /**
   * @brief Initializes the service with configuration settings
   *
   * Creates a service manager and configures it with the provided settings.
   * Sets up HTTP endpoints for:
   * - /cypher (Cypher query execution)
   * - /schema (schema information)
   *
   * @param config Service configuration containing host, port, thread settings,
   * etc.
   *
   * @note This method can be called only once. Subsequent calls are ignored.
   * @note Must be called before Start() or run_and_wait_for_exit()
   */
  void init(const ServiceConfig& config);

  neug::NeugDB& db_;
  neug::NeugDBConfig db_config_;
  std::unique_ptr<neug::TpExecutionSlotPool> execution_slot_pool_;
  std::unique_ptr<ServiceTransactionManager> transaction_manager_;
  std::unique_ptr<IServiceManager> hdl_mgr_;

  std::thread compact_thread_;
  std::atomic<bool> compact_thread_running_{false};
  std::mutex compact_mtx_;
  std::condition_variable compact_cv_;

  std::atomic<bool> running_{false};
  std::mutex mtx_;

  ServiceConfig service_config_;
  bool bthread_runtime_wait_installed_{false};

  friend class neug::NeugDB;
};

}  // namespace neug
