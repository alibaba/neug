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

import java.io.Closeable;

/**
 * The main driver interface for NeuG database connections.
 *
 * <p>A driver manages database connections and provides sessions for executing queries. It is
 * responsible for connection pooling, resource management, and cleanup.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * Driver driver = GraphDatabase.driver("http://localhost:10000");
 * try (Session session = driver.session()) {
 *     ResultSet results = session.run("MATCH (n) RETURN n");
 *     // Process results
 * } finally {
 *     driver.close();
 * }
 * }</pre>
 */
public interface Driver extends Closeable {

    /**
     * Creates a new session for executing queries.
     *
     * <p>Sessions should be closed after use to release resources.
     *
     * @return a new {@link Session} instance
     */
    Session session();

    /**
     * Verifies that the driver can connect to the database server.
     *
     * <p>This method attempts to establish a connection and verify basic connectivity.
     *
     * @throws RuntimeException if the connection cannot be established
     */
    void verifyConnectivity();

    /**
     * Closes the driver and releases all associated resources.
     *
     * <p>After calling this method, the driver should not be used anymore.
     */
    @Override
    void close();

    /**
     * Checks whether the driver has been closed.
     *
     * @return {@code true} if the driver is closed, {@code false} otherwise
     */
    boolean isClosed();
}
