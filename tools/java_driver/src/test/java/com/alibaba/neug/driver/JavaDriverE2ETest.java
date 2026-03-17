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
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import com.alibaba.neug.driver.utils.*;
import java.util.Map;
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

    private static String requireE2EUri() {
        String uri = System.getenv(E2E_URI_ENV);
        assumeTrue(uri != null && !uri.isBlank(), E2E_URI_ENV + " is not set");
        return uri;
    }

    @Test
    public void testDriverCanQueryLiveServer() {
        String uri = requireE2EUri();

        try (Driver driver = GraphDatabase.driver(uri)) {
            assertFalse(driver.isClosed());
            driver.verifyConnectivity();

            try (Session session = driver.session();
                    ResultSet resultSet = session.run("RETURN 1 AS value")) {
                assertTrue(resultSet.next());
                assertEquals(1, resultSet.getInt("value"));
                assertEquals(1L, resultSet.getObject(0));
                assertFalse(resultSet.wasNull());
                assertEquals(Types.INT64, resultSet.getMetaData().getColumnType(0));
                assertEquals("value", resultSet.getMetaData().getColumnName(0));
                assertFalse(resultSet.next());
            }
        }
    }

    @Test
    public void testDriverCanRunParameterizedQuery() {
        String uri = requireE2EUri();

        try (Driver driver = GraphDatabase.driver(uri);
                Session session = driver.session();
                ResultSet resultSet =
                        session.run(
                                "MATCH (n) WHERE n.name = $name RETURN n.age",
                                Map.of("name", "marko"),
                                AccessMode.READ)) {
            assertTrue(resultSet.next());
            assertEquals(29, resultSet.getInt(0));
            assertEquals(29, resultSet.getObject(0));
            assertFalse(resultSet.wasNull());
            assertEquals(Types.INT32, resultSet.getMetaData().getColumnType(0));
            assertEquals("_0_n.age", resultSet.getMetaData().getColumnName(0));
            assertFalse(resultSet.next());
        }
    }
}
