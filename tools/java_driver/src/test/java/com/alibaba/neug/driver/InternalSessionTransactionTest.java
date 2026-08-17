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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.alibaba.neug.driver.internal.InternalSession;
import com.alibaba.neug.driver.utils.Client;
import java.io.IOException;
import org.junit.jupiter.api.Test;

/** Focused transaction state and routing tests for {@link InternalSession}. */
public class InternalSessionTransactionTest {

    private static final String TRANSACTION_ID = "ABCDEFGHIJKLMNOPQRSTUV";
    private static final byte[] QUERY_RESPONSE =
            Results.QueryResponse.getDefaultInstance().toByteArray();

    @Test
    public void transactionRoutesReuseTokenAndCommitClearsState() throws Exception {
        Client client = mock(Client.class);
        when(client.post(eq("/transactions"), any(byte[].class), isNull()))
                .thenReturn(new Client.HttpResponse(200, new byte[0], TRANSACTION_ID));
        when(client.post(eq("/transactions/query"), any(byte[].class), eq(TRANSACTION_ID)))
                .thenReturn(new Client.HttpResponse(200, QUERY_RESPONSE, null));
        when(client.post(eq("/transactions/commit"), any(byte[].class), eq(TRANSACTION_ID)))
                .thenReturn(new Client.HttpResponse(200, new byte[0], null));

        InternalSession session = new InternalSession(client);
        session.beginTransaction();
        assertTrue(session.hasActiveTransaction());
        session.run("RETURN 1").close();
        session.commit();
        assertFalse(session.hasActiveTransaction());

        verify(client).post(eq("/transactions"), any(byte[].class), isNull());
        verify(client).post(eq("/transactions/query"), any(byte[].class), eq(TRANSACTION_ID));
        verify(client).post(eq("/transactions/commit"), any(byte[].class), eq(TRANSACTION_ID));
    }

    @Test
    public void queryFailureRequiresRollbackAndCloseRollsBackActiveState() throws Exception {
        Client client = mock(Client.class);
        when(client.post(eq("/transactions"), any(byte[].class), isNull()))
                .thenReturn(new Client.HttpResponse(200, new byte[0], TRANSACTION_ID));
        when(client.post(eq("/transactions/query"), any(byte[].class), eq(TRANSACTION_ID)))
                .thenReturn(new Client.HttpResponse(500, "failure".getBytes(), null));
        when(client.post(eq("/transactions/rollback"), any(byte[].class), eq(TRANSACTION_ID)))
                .thenReturn(new Client.HttpResponse(200, new byte[0], null));

        InternalSession session = new InternalSession(client);
        session.beginTransaction(TransactionMode.READ_ONLY);
        assertThrows(RuntimeException.class, () -> session.run("RETURN 1"));
        assertTrue(session.hasActiveTransaction());
        assertThrows(IllegalStateException.class, session::commit);
        session.close();
        assertTrue(session.isClosed());

        verify(client).post(eq("/transactions/rollback"), any(byte[].class), eq(TRANSACTION_ID));
    }

    @Test
    public void unknownCommitOutcomeIsTerminalAndIsNotRetried() throws Exception {
        Client client = mock(Client.class);
        when(client.post(eq("/transactions"), any(byte[].class), isNull()))
                .thenReturn(new Client.HttpResponse(200, new byte[0], TRANSACTION_ID));
        when(client.post(eq("/transactions/commit"), any(byte[].class), eq(TRANSACTION_ID)))
                .thenThrow(new IOException("response lost"));

        InternalSession session = new InternalSession(client);
        session.beginTransaction(TransactionMode.READ_WRITE);
        assertThrows(RuntimeException.class, session::commit);
        assertFalse(session.hasActiveTransaction());
        assertThrows(IllegalStateException.class, session::rollback);
        assertThrows(IllegalStateException.class, () -> session.run("RETURN 1"));
        session.close();

        verify(client).post(eq("/transactions/commit"), any(byte[].class), eq(TRANSACTION_ID));
    }

    @Test
    public void rejectedCommitRemainsRollbackable() throws Exception {
        Client client = mock(Client.class);
        when(client.post(eq("/transactions"), any(byte[].class), isNull()))
                .thenReturn(new Client.HttpResponse(200, new byte[0], TRANSACTION_ID));
        when(client.post(eq("/transactions/commit"), any(byte[].class), eq(TRANSACTION_ID)))
                .thenReturn(new Client.HttpResponse(500, "failure".getBytes(), null));
        when(client.post(eq("/transactions/rollback"), any(byte[].class), eq(TRANSACTION_ID)))
                .thenReturn(new Client.HttpResponse(200, new byte[0], null));

        InternalSession session = new InternalSession(client);
        session.beginTransaction(TransactionMode.READ_WRITE);
        assertThrows(RuntimeException.class, session::commit);
        assertTrue(session.hasActiveTransaction());
        session.rollback();
        assertFalse(session.hasActiveTransaction());

        verify(client).post(eq("/transactions/rollback"), any(byte[].class), eq(TRANSACTION_ID));
    }
}
