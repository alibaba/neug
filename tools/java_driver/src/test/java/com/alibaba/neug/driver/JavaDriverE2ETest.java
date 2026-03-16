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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.alibaba.neug.driver.utils.Types;
import org.junit.jupiter.api.Test;

/**
 * End-to-end integration test for the Java driver.
 *
 * <p>This test is skipped unless `NEUG_JAVA_DRIVER_E2E_URI` is set, so it is safe to keep in the
 * default test suite. Example:
 *
 * <pre>{@code
 * NEUG_JAVA_DRIVER_E2E_URI=http://localhost:10000 mvn -Dtest=JavaDriverE2ETest test
 * }</pre>
 */
public class JavaDriverE2ETest {

    private static final String E2E_URI_ENV = "NEUG_JAVA_DRIVER_E2E_URI";

    @Test
    public void testDriverCanQueryLiveServer() {
        String uri = System.getenv(E2E_URI_ENV);
        assumeTrue(uri != null && !uri.isBlank(), E2E_URI_ENV + " is not set");

        try (Driver driver = GraphDatabase.driver(uri)) {
            assertFalse(driver.isClosed());
            driver.verifyConnectivity();

            try (Session session = driver.session();
                    ResultSet resultSet = session.run("RETURN 1 AS value")) {
                assertTrue(resultSet.next());
                assertEquals(1, resultSet.getInt("value"));
                assertEquals(1, resultSet.getObject(0));
                assertFalse(resultSet.wasNull());
                assertEquals(Types.INT32, resultSet.getMetaData().getColumnType(0));
                assertEquals("value", resultSet.getMetaData().getColumnName(0));
                assertFalse(resultSet.next());
            }
        }
    }
}
