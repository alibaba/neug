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

/** Test class for {@link GraphDatabase}. */
public class GraphDatabaseTest {

    @Test
    public void testDriverCreationWithUri() {
        String uri = "http://localhost:8000";
        Driver driver = GraphDatabase.driver(uri);

        assertNotNull(driver);
        assertFalse(driver.isClosed());

        driver.close();
        assertTrue(driver.isClosed());
    }

    @Test
    public void testDriverCreationWithUriAndConfig() {
        String uri = "http://localhost:8000";
        Config config = Config.builder().withConnectionTimeoutMillis(5000).build();

        Driver driver = GraphDatabase.driver(uri, config);

        assertNotNull(driver);
        assertFalse(driver.isClosed());

        driver.close();
    }

    @Test
    public void testDriverCreationWithNullUri() {
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    GraphDatabase.driver(null);
                });
    }

    @Test
    public void testDriverCreationWithEmptyUri() {
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    GraphDatabase.driver("");
                });
    }

    @Test
    public void testMultipleDriverInstances() {
        Driver driver1 = GraphDatabase.driver("http://localhost:8000");
        Driver driver2 = GraphDatabase.driver("http://localhost:9000");

        assertNotNull(driver1);
        assertNotNull(driver2);
        assertNotSame(driver1, driver2);

        driver1.close();
        driver2.close();
    }
}
