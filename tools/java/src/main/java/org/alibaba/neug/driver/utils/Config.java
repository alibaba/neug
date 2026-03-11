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
package org.alibaba.neug.driver.utils;

import java.io.Serializable;

public final class Config implements Serializable {
    private static final long serialVersionUID = 1L;

    public static final class ConfigBuilder {
        private int connectionTimeoutMillis = 30000;
        private int readTimeoutMillis = 30000;
        private int writeTimeoutMillis = 30000;
        private int keepAliveIntervalMillis = 30000;
        private int maxConnectionPoolSize = 100;
        private int maxRequestsPerHost = 1000;
        private int maxRequests = 10000;

        public ConfigBuilder withConnectionTimeoutMillis(int connectionTimeoutMillis) {
            this.connectionTimeoutMillis = connectionTimeoutMillis;
            return this;
        }

        public ConfigBuilder withReadTimeoutMillis(int readTimeoutMillis) {
            this.readTimeoutMillis = readTimeoutMillis;
            return this;
        }

        public ConfigBuilder withWriteTimeoutMillis(int writeTimeoutMillis) {
            this.writeTimeoutMillis = writeTimeoutMillis;
            return this;
        }

        public ConfigBuilder withKeepAliveIntervalMillis(int keepAliveIntervalMillis) {
            this.keepAliveIntervalMillis = keepAliveIntervalMillis;
            return this;
        }

        public ConfigBuilder withMaxConnectionPoolSize(int maxConnectionPoolSize) {
            this.maxConnectionPoolSize = maxConnectionPoolSize;
            return this;
        }

        public ConfigBuilder withMaxRequestsPerHost(int maxRequestsPerHost) {
            this.maxRequestsPerHost = maxRequestsPerHost;
            return this;
        }

        public ConfigBuilder withMaxRequests(int maxRequests) {
            this.maxRequests = maxRequests;
            return this;
        }

        public Config build() {
            Config config = new Config();
            config.connectionTimeoutMillis = connectionTimeoutMillis;
            config.readTimeoutMillis = readTimeoutMillis;
            config.writeTimeoutMillis = writeTimeoutMillis;
            config.keepAliveIntervalMillis = keepAliveIntervalMillis;
            config.maxConnectionPoolSize = maxConnectionPoolSize;
            config.maxRequestsPerHost = maxRequestsPerHost;
            config.maxRequests = maxRequests;
            return config;
        }
    }
    ;

    public static ConfigBuilder builder() {
        return new ConfigBuilder();
    }

    public int getConnectionTimeoutMillis() {
        return connectionTimeoutMillis;
    }

    public int getReadTimeoutMillis() {
        return readTimeoutMillis;
    }

    public int getWriteTimeoutMillis() {
        return writeTimeoutMillis;
    }

    public int getKeepAliveIntervalMillis() {
        return keepAliveIntervalMillis;
    }

    public int getMaxConnectionPoolSize() {
        return maxConnectionPoolSize;
    }

    public int getMaxRequestsPerHost() {
        return maxRequestsPerHost;
    }

    public int getMaxRequests() {
        return maxRequests;
    }

    private int connectionTimeoutMillis;
    private int readTimeoutMillis;
    private int writeTimeoutMillis;
    private int keepAliveIntervalMillis;
    private int maxConnectionPoolSize;
    private int maxRequestsPerHost;
    private int maxRequests;
}
