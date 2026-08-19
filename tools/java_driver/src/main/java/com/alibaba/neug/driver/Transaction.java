/**
 * Copyright 2020 Alibaba Group Holding Limited.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.neug.driver;

import java.util.Map;

public interface Transaction extends AutoCloseable {
    /** Access permission fixed for the lifetime of an explicit transaction. */
    enum Mode {
        READ_ONLY,
        READ_WRITE
    }

    /**
     * Executes a Cypher statement within the transaction and returns the results.
     *
     * @param statement the Cypher query to execute
     * @return a {@link ResultSet} containing the query results
     * @throws RuntimeException if the query fails
     */
    ResultSet run(String statement);

    /**
     * Executes a Cypher statement with parameters within the transaction and returns the results.
     *
     * @param statement the Cypher query to execute
     * @param parameters query parameters as key-value pairs
     * @return a {@link ResultSet} containing the query results
     * @throws RuntimeException if the query fails
     */
    ResultSet run(String statement, Map<String, Object> parameters);

    /**
     * Commits the transaction, making all changes permanent.
     *
     * @throws RuntimeException if the commit fails
     */
    void commit();

    /**
     * Rolls back the transaction, discarding all changes made during the transaction.
     *
     * @throws RuntimeException if the rollback fails
     */
    void rollback();

    /**
     * Closes the transaction, releasing any resources held by it. If the transaction has not been
     * committed or rolled back, it will be rolled back automatically.
     *
     * @throws RuntimeException if closing the transaction fails
     */
    @Override
    void close();

    /**
     * Checks if the transaction is still open.
     *
     * @return true if the transaction is open, false otherwise
     */
    boolean isOpen();
}
