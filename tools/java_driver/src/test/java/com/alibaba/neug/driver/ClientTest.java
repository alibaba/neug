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

import com.alibaba.neug.driver.utils.Client;
import com.alibaba.neug.driver.utils.Config;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Test class for {@link Client}. */
public class ClientTest {

    @Test
    public void testClientConstruction() {
        Config config = Config.builder().build();
        Client client = new Client("http://localhost:8080", config);
        assertNotNull(client);
        assertFalse(client.isClosed());
    }

    @Test
    public void testClientClose() {
        Config config = Config.builder().build();
        Client client = new Client("http://localhost:8080", config);
        assertFalse(client.isClosed());
        client.close();
        assertTrue(client.isClosed());
    }

    @Test
    public void testClientWithCustomConfig() {
        Config config =
                Config.builder()
                        .withMaxConnectionPoolSize(20)
                        .withConnectionTimeoutMillis(5000)
                        .withReadTimeoutMillis(30000)
                        .withWriteTimeoutMillis(30000)
                        .withKeepAliveIntervalMillis(300000)
                        .build();
        Client client = new Client("http://localhost:8080", config);
        assertNotNull(client);
        assertFalse(client.isClosed());
    }

    @Test
    public void testSyncPostThrowsExceptionWhenServerUnreachable() {
        Config config =
                Config.builder()
                        .withConnectionTimeoutMillis(1000) // Short timeout for faster test
                        .build();
        Client client = new Client("http://localhost:19999", config); // Non-existent server

        byte[] request = "test query".getBytes();
        assertThrows(IOException.class, () -> client.syncPost(request));
    }

    @Test
    public void testMultipleClientsIndependence() {
        Config config1 = Config.builder().build();
        Config config2 = Config.builder().build();

        Client client1 = new Client("http://localhost:8080", config1);
        Client client2 = new Client("http://localhost:9090", config2);

        assertNotNull(client1);
        assertNotNull(client2);
        assertFalse(client1.isClosed());
        assertFalse(client2.isClosed());

        client1.close();
        assertTrue(client1.isClosed());
        assertFalse(client2.isClosed());

        client2.close();
        assertTrue(client2.isClosed());
    }
}
