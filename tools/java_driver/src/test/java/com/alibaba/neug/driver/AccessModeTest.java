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

import com.alibaba.neug.driver.utils.AccessMode;
import org.junit.jupiter.api.Test;

/** Test class for {@link AccessMode}. */
public class AccessModeTest {

    @Test
    public void testAccessModeValues() {
        assertNotNull(AccessMode.READ);
        assertNotNull(AccessMode.INSERT);
        assertNotNull(AccessMode.UPDATE);
        assertNotNull(AccessMode.SCHEMA);
    }

    @Test
    public void testAccessModeValueOf() {
        assertEquals(AccessMode.READ, AccessMode.valueOf("READ"));
        assertEquals(AccessMode.INSERT, AccessMode.valueOf("INSERT"));
        assertEquals(AccessMode.UPDATE, AccessMode.valueOf("UPDATE"));
        assertEquals(AccessMode.SCHEMA, AccessMode.valueOf("SCHEMA"));
    }

    @Test
    public void testAccessModeName() {
        assertEquals("READ", AccessMode.READ.name());
        assertEquals("INSERT", AccessMode.INSERT.name());
        assertEquals("UPDATE", AccessMode.UPDATE.name());
        assertEquals("SCHEMA", AccessMode.SCHEMA.name());
    }

    @Test
    public void testInvalidAccessMode() {
        assertThrows(
                IllegalArgumentException.class,
                () -> {
                    AccessMode.valueOf("INVALID");
                });
    }
}
