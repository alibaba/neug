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

import static org.junit.jupiter.api.Assertions.*;

import com.alibaba.neug.driver.utils.Config;
import org.junit.jupiter.api.Test;

/** Test class for {@link Config}. */
public class ConfigTest {

    @Test
    public void testDefaultConfig() {
        Config config = Config.builder().build();

        assertEquals(30000, config.getConnectionTimeoutMillis());
        assertEquals(30000, config.getReadTimeoutMillis());
        assertEquals(30000, config.getWriteTimeoutMillis());
        assertEquals(30000, config.getKeepAliveIntervalMillis());
        assertEquals(100, config.getMaxConnectionPoolSize());
    }

    @Test
    public void testCustomConfig() {
        Config config =
                Config.builder()
                        .withConnectionTimeoutMillis(10000)
                        .withReadTimeoutMillis(20000)
                        .withWriteTimeoutMillis(15000)
                        .withKeepAliveIntervalMillis(60000)
                        .withMaxConnectionPoolSize(50)
                        .build();

        assertEquals(10000, config.getConnectionTimeoutMillis());
        assertEquals(20000, config.getReadTimeoutMillis());
        assertEquals(15000, config.getWriteTimeoutMillis());
        assertEquals(60000, config.getKeepAliveIntervalMillis());
        assertEquals(50, config.getMaxConnectionPoolSize());
    }

    @Test
    public void testBuilderChaining() {
        Config.ConfigBuilder builder = Config.builder();
        Config config =
                builder.withConnectionTimeoutMillis(5000)
                        .withReadTimeoutMillis(5000)
                        .withWriteTimeoutMillis(5000)
                        .build();

        assertNotNull(config);
        assertEquals(5000, config.getConnectionTimeoutMillis());
    }

    @Test
    public void testConfigImmutability() {
        Config config1 = Config.builder().withConnectionTimeoutMillis(10000).build();
        Config config2 = Config.builder().withConnectionTimeoutMillis(20000).build();

        assertEquals(10000, config1.getConnectionTimeoutMillis());
        assertEquals(20000, config2.getConnectionTimeoutMillis());
    }
}
