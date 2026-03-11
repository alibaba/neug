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
package org.alibaba.neug.driver;

import org.alibaba.neug.driver.internal.InternalDriver;
import org.alibaba.neug.driver.utils.Config;

/**
 * Main entry point for creating NeuG database driver connections.
 *
 * <p>This class provides factory methods to create {@link Driver} instances that can be used to
 * interact with a NeuG graph database.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create a driver with default configuration
 * Driver driver = GraphDatabase.driver("http://localhost:8080");
 *
 * // Create a driver with custom configuration
 * Config config = Config.builder()
 *     .withMaxConnectionPoolSize(10)
 *     .build();
 * Driver driver = GraphDatabase.driver("http://localhost:8080", config);
 * }</pre>
 */
public final class GraphDatabase {

    private GraphDatabase() {
        // Prevent instantiation
    }

    /**
     * Creates a new driver instance with default configuration.
     *
     * @param uri the URI of the NeuG database server (e.g., "http://localhost:8080")
     * @return a new {@link Driver} instance
     * @throws IllegalArgumentException if the URI is null or invalid
     */
    public static Driver driver(String uri) {
        return new InternalDriver(uri, Config.builder().build());
    }

    /**
     * Creates a new driver instance with custom configuration.
     *
     * @param uri the URI of the NeuG database server (e.g., "http://localhost:8080")
     * @param config the configuration settings for the driver
     * @return a new {@link Driver} instance
     * @throws IllegalArgumentException if the URI or config is null or invalid
     */
    public static Driver driver(String uri, Config config) {
        return new InternalDriver(uri, config);
    }
}
;
