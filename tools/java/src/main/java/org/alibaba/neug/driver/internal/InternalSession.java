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
package org.alibaba.neug.driver.internal;

import java.util.Map;
import org.alibaba.neug.driver.ResultSet;
import org.alibaba.neug.driver.Session;
import org.alibaba.neug.driver.utils.AccessMode;
import org.alibaba.neug.driver.utils.Client;
import org.alibaba.neug.driver.utils.QuerySerializer;
import org.alibaba.neug.driver.utils.ResponseParser;

public class InternalSession implements Session {

    private final Client client;

    public InternalSession(Client client) {
        this.client = client;
    }

    @Override
    public ResultSet run(String query) {
        try {
            byte[] request = QuerySerializer.serialize(query);
            byte[] response = client.syncPost(request);
            return ResponseParser.parse(response);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute query", e);
        }
    }

    @Override
    public ResultSet run(String query, Map<String, Object> parameters) {
        try {
            byte[] request = QuerySerializer.serialize(query, parameters);
            byte[] response = client.syncPost(request);
            return ResponseParser.parse(response);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute query", e);
        }
    }

    @Override
    public ResultSet run(String query, AccessMode mode) {
        try {
            byte[] request = QuerySerializer.serialize(query, mode);
            byte[] response = client.syncPost(request);
            return ResponseParser.parse(response);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute query", e);
        }
    }

    @Override
    public ResultSet run(String query, Map<String, Object> parameters, AccessMode mode) {
        try {
            byte[] request = QuerySerializer.serialize(query, parameters, mode);
            byte[] response = client.syncPost(request);
            return ResponseParser.parse(response);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute query", e);
        }
    }

    @Override
    public void close() {}

    @Override
    public boolean isClosed() {
        return false;
    }
}
