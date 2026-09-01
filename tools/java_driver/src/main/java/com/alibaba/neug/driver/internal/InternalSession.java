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
package com.alibaba.neug.driver.internal;

import com.alibaba.neug.driver.ResultSet;
import com.alibaba.neug.driver.Session;
import com.alibaba.neug.driver.Transaction;
import com.alibaba.neug.driver.utils.AccessMode;
import com.alibaba.neug.driver.utils.Client;
import com.alibaba.neug.driver.utils.QuerySerializer;
import com.alibaba.neug.driver.utils.ResponseParser;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import okhttp3.HttpUrl;

/**
 * Internal implementation of the {@link Session} interface.
 *
 * <p>This class handles query execution by serializing queries, sending them to the database server
 * via HTTP, and parsing the responses into ResultSet objects.
 */
public class InternalSession implements Session {

    private final Client client;
    private final HttpUrl cypherEndpoint;
    private final HttpUrl transactionEndpoint;
    private boolean closed;
    private InternalTransaction activeTransaction;

    /**
     * Constructs a new InternalSession with the specified client.
     *
     * @param client the HTTP client used to communicate with the database
     */
    public InternalSession(Client client) {
        this.client = client;
        this.cypherEndpoint = client.endpoint("cypher");
        this.transactionEndpoint = client.endpoint("transactions");
        this.closed = false;
        this.activeTransaction = null;
    }

    @Override
    public ResultSet run(String query) {
        return run(query, null, null);
    }

    @Override
    public ResultSet run(String query, Map<String, Object> parameters) {
        return run(query, parameters, null);
    }

    @Override
    public ResultSet run(String query, AccessMode mode) {
        return run(query, null, mode);
    }

    @Override
    public ResultSet run(String query, Map<String, Object> parameters, AccessMode mode) {
        if (closed) {
            throw new IllegalStateException("Session is already closed");
        }
        ensureNoActiveTransaction();
        try {
            byte[] request = QuerySerializer.serialize(query, parameters, mode);
            Client.HttpResponse response = client.syncPost(cypherEndpoint, request);
            if (!response.isSuccessful()) {
                throw httpFailure("execute query", response);
            }
            return ResponseParser.parse(response.getBody());
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute query", e);
        }
    }

    @Override
    public Transaction beginTransaction(Transaction.Mode mode) {
        if (closed) {
            throw new IllegalStateException("Session is already closed");
        }
        ensureNoActiveTransaction();
        try {
            Objects.requireNonNull(mode, "mode");
            String value = mode == Transaction.Mode.READ_ONLY ? "read_only" : "read_write";
            byte[] request = ("{\"mode\":\"" + value + "\"}").getBytes(StandardCharsets.UTF_8);
            Client.HttpResponse response = client.syncPost(transactionEndpoint, request);
            if (!response.isSuccessful()) {
                throw httpFailure("begin transaction", response);
            }
            String transactionId = ResponseParser.parseTransactionId(response.getBody());
            activeTransaction = new InternalTransaction(client, transactionId);
            return activeTransaction;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to begin transaction", e);
        }
    }

    @Override
    public void close() {
        if (!closed) {
            try {
                if (activeTransaction != null && activeTransaction.isOpen()) {
                    activeTransaction.close();
                }
            } catch (Exception e) {
                // The server-side transaction deadline provides final cleanup.
            } finally {
                closed = true;
            }
        }
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    private void ensureNoActiveTransaction() {
        if (activeTransaction != null) {
            activeTransaction.ensureSessionReusable();
        }
    }

    private RuntimeException httpFailure(String operation, Client.HttpResponse response) {
        String body = new String(response.getBody(), StandardCharsets.UTF_8);
        return new RuntimeException(
                "Failed to " + operation + ": HTTP " + response.getStatusCode() + " " + body);
    }
}
