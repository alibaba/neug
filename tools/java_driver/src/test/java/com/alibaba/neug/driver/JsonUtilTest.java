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

import com.alibaba.neug.driver.utils.JsonUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

/** Test class for {@link JsonUtil}. */
public class JsonUtilTest {

    @Test
    public void testGetInstance() {
        ObjectMapper mapper = JsonUtil.getInstance();
        assertNotNull(mapper);
    }

    @Test
    public void testSingletonPattern() {
        ObjectMapper mapper1 = JsonUtil.getInstance();
        ObjectMapper mapper2 = JsonUtil.getInstance();

        assertSame(mapper1, mapper2, "Should return the same instance");
    }

    @Test
    public void testObjectMapperFunctionality() throws Exception {
        ObjectMapper mapper = JsonUtil.getInstance();

        String json = "{\"name\":\"test\",\"value\":123}";
        Object obj = mapper.readValue(json, Object.class);

        assertNotNull(obj);
    }
}
