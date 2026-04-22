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
import com.alibaba.neug.driver.utils.QuerySerializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Test class for {@link QuerySerializer}. */
public class QuerySerializerTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    public void testSerializeSimpleQuery() throws Exception {
        String query = "MATCH (n) RETURN n";
        byte[] result = QuerySerializer.serialize(query);

        assertNotNull(result);
        assertTrue(result.length > 0);

        JsonNode json = mapper.readTree(result);
        assertEquals(query, json.get("query").asText());
        assertNull(json.get("parameters"));
        assertNull(json.get("access_mode"));
    }

    @Test
    public void testSerializeQueryWithParameters() throws Exception {
        String query = "MATCH (n:Person {name: $name}) RETURN n";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("name", "Alice");
        parameters.put("age", 30);

        byte[] result = QuerySerializer.serialize(query, parameters);

        assertNotNull(result);
        JsonNode json = mapper.readTree(result);
        assertEquals(query, json.get("query").asText());
        assertEquals("Alice", json.get("parameters").get("name").asText());
        assertEquals(30, json.get("parameters").get("age").asInt());
    }

    @Test
    public void testSerializeQueryWithAccessMode() throws Exception {
        String query = "MATCH (n) RETURN n";
        byte[] result = QuerySerializer.serialize(query, AccessMode.READ);

        assertNotNull(result);
        JsonNode json = mapper.readTree(result);
        assertEquals(query, json.get("query").asText());
        assertEquals("READ", json.get("access_mode").asText());
    }

    @Test
    public void testSerializeQueryWithParametersAndAccessMode() throws Exception {
        String query = "CREATE (n:Person {name: $name}) RETURN n";
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("name", "Bob");

        byte[] result = QuerySerializer.serialize(query, parameters, AccessMode.INSERT);

        assertNotNull(result);
        JsonNode json = mapper.readTree(result);
        assertEquals(query, json.get("query").asText());
        assertEquals("Bob", json.get("parameters").get("name").asText());
        assertEquals("INSERT", json.get("access_mode").asText());
    }

    @Test
    public void testSerializeQueryWithComplexParameters() throws Exception {
        String query = "CREATE (n:Person $props) RETURN n";
        Map<String, Object> parameters = new HashMap<>();
        Map<String, Object> props = new HashMap<>();
        props.put("name", "Charlie");
        props.put("age", 25);
        props.put("active", true);
        parameters.put("props", props);

        byte[] result = QuerySerializer.serialize(query, parameters);

        assertNotNull(result);
        JsonNode json = mapper.readTree(result);
        JsonNode propsNode = json.get("parameters").get("props");
        assertEquals("Charlie", propsNode.get("name").asText());
        assertEquals(25, propsNode.get("age").asInt());
        assertTrue(propsNode.get("active").asBoolean());
    }

    @Test
    public void testSerializeWithNullParameters() throws Exception {
        String query = "MATCH (n) RETURN n";
        Map<String, Object> nullParams = null;
        byte[] result = QuerySerializer.serialize(query, nullParams);

        assertNotNull(result);
        JsonNode json = mapper.readTree(result);
        assertEquals(query, json.get("query").asText());
        assertNull(json.get("parameters"));
    }

    @Test
    public void testSerializeWithEmptyParameters() throws Exception {
        String query = "MATCH (n) RETURN n";
        Map<String, Object> parameters = new HashMap<>();
        byte[] result = QuerySerializer.serialize(query, parameters);

        assertNotNull(result);
        JsonNode json = mapper.readTree(result);
        assertEquals(query, json.get("query").asText());
        assertTrue(json.get("parameters").isEmpty());
    }
}
