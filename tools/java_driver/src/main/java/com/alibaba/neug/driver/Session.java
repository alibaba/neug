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

import com.alibaba.neug.driver.utils.AccessMode;
import java.util.Map;

/**
 * A session for executing queries against a NeuG database.
 *
 * <p>Sessions are lightweight and should be closed after use. They provide methods for executing
 * Cypher queries and retrieving results.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * try (Session session = driver.session()) {
 *     ResultSet results = session.run("MATCH (n:Person) WHERE n.age > $age RETURN n",
 *         Map.of("age", 30));
 *     while (results.next()) {
 *         System.out.println(results.getObject("n").toString());
 *     }
 * }
 * }</pre>
 */
public interface Session extends AutoCloseable {
    /**
     * Executes a Cypher statement and returns the results.
     *
     * @param statement the Cypher query to execute
     * @return a {@link ResultSet} containing the query results
     * @throws RuntimeException if the query fails
     */
    ResultSet run(String statement);

    /**
     * Executes a Cypher statement with configuration options.
     *
     * @param statement the Cypher query to execute
     * @param parameters query parameters as key-value pairs
     * @return a {@link ResultSet} containing the query results
     * @throws RuntimeException if the query fails
     */
    ResultSet run(String statement, Map<String, Object> parameters);

    /**
     * Executes a Cypher statement with a specific access mode.
     *
     * @param statement the Cypher query to execute
     * @param mode the access mode (READ/INSERT/UPDATE/SCHEMA)
     * @return a {@link ResultSet} containing the query results
     * @throws RuntimeException if the query fails
     */
    ResultSet run(String statement, AccessMode mode);

    /**
     * Executes a parameterized Cypher statement with a specific access mode.
     *
     * @param statement the Cypher query to execute
     * @param parameters query parameters as key-value pairs
     * @param mode the access mode (READ/INSERT/UPDATE/SCHEMA)
     * @return a {@link ResultSet} containing the query results
     * @throws RuntimeException if the query fails
     */
    ResultSet run(String statement, Map<String, Object> parameters, AccessMode mode);

    /** Closes the session and releases all associated resources. */
    @Override
    void close();

    /**
     * Checks whether the session has been closed.
     *
     * @return {@code true} if the session is closed, {@code false} otherwise
     */
    boolean isClosed();
}
