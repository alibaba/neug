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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Map;

/**
 * Utility class for serializing queries to JSON format for transmission to the database server.
 *
 * <p>This class converts query strings, parameters, and access modes into JSON bytes that can be
 * sent over HTTP.
 */
public class QuerySerializer {

    /**
     * Serializes a query without parameters or access mode.
     *
     * @param query the query string
     * @return the serialized query as a byte array
     */
    public static byte[] serialize(String query) {
        return serialize(query, null, null);
    }

    /**
     * Serializes a query with parameters but without access mode.
     *
     * @param query the query string
     * @param parameters the query parameters as a map of name-value pairs
     * @return the serialized query as a byte array
     */
    public static byte[] serialize(String query, Map<String, Object> parameters) {
        return serialize(query, parameters, null);
    }

    /**
     * Serializes a query with access mode but without parameters.
     *
     * @param query the query string
     * @param accessMode the access mode for the query
     * @return the serialized query as a byte array
     */
    public static byte[] serialize(String query, AccessMode accessMode) {
        return serialize(query, null, accessMode);
    }

    /**
     * Serializes a query with parameters and access mode.
     *
     * @param query the query string
     * @param parameters the query parameters as a map of name-value pairs (nullable)
     * @param accessMode the access mode for the query (nullable)
     * @return the serialized query as a byte array
     */
    public static byte[] serialize(
            String query, Map<String, Object> parameters, AccessMode accessMode) {
        try {
            ObjectMapper mapper = JsonUtil.getInstance();
            ObjectNode root = mapper.createObjectNode();
            root.put("query", query);
            if (parameters != null) {
                ObjectNode paramsNode = mapper.createObjectNode();
                for (Map.Entry<String, Object> entry : parameters.entrySet()) {
                    paramsNode.putPOJO(entry.getKey(), entry.getValue());
                }
                root.set("parameters", paramsNode);
            }
            if (accessMode != null) {
                root.put("access_mode", accessMode.name());
            }
            return mapper.writeValueAsBytes(root);
        } catch (Exception e) {
            throw new RuntimeException("Failed to serialize query", e);
        }
    }
}
