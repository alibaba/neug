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
package com.alibaba.neug.driver.utils;

import java.io.Serializable;

/**
 * Configuration for the NeuG driver.
 *
 * <p>This class holds various timeout and connection pool settings. Use the {@link ConfigBuilder}
 * to create instances.
 */
public final class Config implements Serializable {
    private static final long serialVersionUID = 1L;

    /** Builder for creating {@link Config} instances with custom settings. */
    public static final class ConfigBuilder {
        private int connectionTimeoutMillis = 30000;
        private int readTimeoutMillis = 30000;
        private int writeTimeoutMillis = 30000;
        private int keepAliveIntervalMillis = 30000;
        private int maxConnectionPoolSize = 100;

        /**
         * Sets the connection timeout in milliseconds.
         *
         * @param connectionTimeoutMillis the connection timeout
         * @return this builder
         */
        public ConfigBuilder withConnectionTimeoutMillis(int connectionTimeoutMillis) {
            this.connectionTimeoutMillis = connectionTimeoutMillis;
            return this;
        }

        /**
         * Sets the read timeout in milliseconds.
         *
         * @param readTimeoutMillis the read timeout
         * @return this builder
         */
        public ConfigBuilder withReadTimeoutMillis(int readTimeoutMillis) {
            this.readTimeoutMillis = readTimeoutMillis;
            return this;
        }

        /**
         * Sets the write timeout in milliseconds.
         *
         * @param writeTimeoutMillis the write timeout
         * @return this builder
         */
        public ConfigBuilder withWriteTimeoutMillis(int writeTimeoutMillis) {
            this.writeTimeoutMillis = writeTimeoutMillis;
            return this;
        }

        /**
         * Sets the keep-alive interval in milliseconds.
         *
         * @param keepAliveIntervalMillis the keep-alive interval
         * @return this builder
         */
        public ConfigBuilder withKeepAliveIntervalMillis(int keepAliveIntervalMillis) {
            this.keepAliveIntervalMillis = keepAliveIntervalMillis;
            return this;
        }

        /**
         * Sets the maximum connection pool size.
         *
         * @param maxConnectionPoolSize the maximum number of connections in the pool
         * @return this builder
         */
        public ConfigBuilder withMaxConnectionPoolSize(int maxConnectionPoolSize) {
            this.maxConnectionPoolSize = maxConnectionPoolSize;
            return this;
        }

        /**
         * Builds a new {@link Config} instance with the configured settings.
         *
         * @return a new Config instance
         */
        public Config build() {
            Config config = new Config();
            config.connectionTimeoutMillis = connectionTimeoutMillis;
            config.readTimeoutMillis = readTimeoutMillis;
            config.writeTimeoutMillis = writeTimeoutMillis;
            config.keepAliveIntervalMillis = keepAliveIntervalMillis;
            config.maxConnectionPoolSize = maxConnectionPoolSize;
            return config;
        }
    }

    /**
     * Creates a new ConfigBuilder for constructing Config instances.
     *
     * @return a new ConfigBuilder
     */
    public static ConfigBuilder builder() {
        return new ConfigBuilder();
    }

    /**
     * Gets the connection timeout in milliseconds.
     *
     * @return the connection timeout
     */
    public int getConnectionTimeoutMillis() {
        return connectionTimeoutMillis;
    }

    /**
     * Gets the read timeout in milliseconds.
     *
     * @return the read timeout
     */
    public int getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    /**
     * Gets the write timeout in milliseconds.
     *
     * @return the write timeout
     */
    public int getWriteTimeoutMillis() {
        return writeTimeoutMillis;
    }

    /**
     * Gets the keep-alive interval in milliseconds.
     *
     * @return the keep-alive interval
     */
    public int getKeepAliveIntervalMillis() {
        return keepAliveIntervalMillis;
    }

    /**
     * Gets the maximum connection pool size.
     *
     * @return the maximum connection pool size
     */
    public int getMaxConnectionPoolSize() {
        return maxConnectionPoolSize;
    }

    private int connectionTimeoutMillis;
    private int readTimeoutMillis;
    private int writeTimeoutMillis;
    private int keepAliveIntervalMillis;
    private int maxConnectionPoolSize;
}
