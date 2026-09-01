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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.alibaba.neug.driver.internal.InternalSession;
import com.alibaba.neug.driver.utils.Client;
import com.alibaba.neug.driver.utils.Config;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import okhttp3.HttpUrl;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests explicit transaction ownership and terminal state handling. */
public class TransactionTest {

    private static final String TRANSACTION_ID = "ABCDEFGHIJKLMNOPQRSTUV";

    private FakeClient client;

    @BeforeEach
    public void setUp() {
        client = new FakeClient();
    }

    @AfterEach
    public void tearDown() {
        client.close();
    }

    @Test
    public void sessionAllowsOnlyOneActiveTransaction() {
        InternalSession session = new InternalSession(client);

        session.beginTransaction();

        assertThrows(IllegalStateException.class, session::beginTransaction);
        assertThrows(IllegalStateException.class, () -> session.run("RETURN 1"));
    }

    @Test
    public void commitReleasesSessionForNextTransaction() {
        InternalSession session = new InternalSession(client);
        Transaction transaction = session.beginTransaction();

        transaction.commit();

        assertFalse(transaction.isOpen());
        assertTrue(session.beginTransaction().isOpen());
    }

    @Test
    public void lostCommitResponseBlocksFurtherSessionUse() {
        client.failCommit = true;
        InternalSession session = new InternalSession(client);
        Transaction transaction = session.beginTransaction();

        assertThrows(RuntimeException.class, transaction::commit);

        assertFalse(transaction.isOpen());
        IllegalStateException beginError =
                assertThrows(IllegalStateException.class, session::beginTransaction);
        IllegalStateException runError =
                assertThrows(IllegalStateException.class, () -> session.run("RETURN 1"));
        assertEquals(
                "Transaction outcome is unknown; close this session and create a new one",
                beginError.getMessage());
        assertEquals(beginError.getMessage(), runError.getMessage());
    }

    @Test
    public void busyCommitLeavesTransactionRollbackOnly() {
        client.busyCommit = true;
        InternalSession session = new InternalSession(client);
        Transaction transaction = session.beginTransaction();

        RuntimeException error = assertThrows(RuntimeException.class, transaction::commit);

        assertTrue(error.getMessage().contains("HTTP 409 transaction busy"));
        assertTrue(transaction.isOpen());
        assertThrows(IllegalStateException.class, session::beginTransaction);

        client.busyCommit = false;
        transaction.rollback();
        assertTrue(session.beginTransaction().isOpen());
    }

    @Test
    public void expiredCommitClosesTransactionAndReleasesSession() {
        client.expireCommit = true;
        InternalSession session = new InternalSession(client);
        Transaction transaction = session.beginTransaction();

        RuntimeException error = assertThrows(RuntimeException.class, transaction::commit);

        assertTrue(error.getMessage().contains("HTTP 410 transaction expired"));
        assertFalse(transaction.isOpen());
        assertTrue(session.beginTransaction().isOpen());
    }

    @Test
    public void rejectedRollbackClosesTransactionAndReleasesSession() {
        client.rejectRollback = true;
        InternalSession session = new InternalSession(client);
        Transaction transaction = session.beginTransaction();

        RuntimeException error = assertThrows(RuntimeException.class, transaction::rollback);

        assertTrue(error.getMessage().contains("HTTP 410 transaction expired"));
        assertFalse(transaction.isOpen());
        assertTrue(session.beginTransaction().isOpen());
    }

    @Test
    public void busyRollbackCanBeRetried() {
        client.busyRollback = true;
        InternalSession session = new InternalSession(client);
        Transaction transaction = session.beginTransaction();

        RuntimeException error = assertThrows(RuntimeException.class, transaction::rollback);

        assertTrue(error.getMessage().contains("HTTP 409 transaction busy"));
        assertTrue(transaction.isOpen());

        client.busyRollback = false;
        transaction.rollback();
        assertFalse(transaction.isOpen());
        assertTrue(session.beginTransaction().isOpen());
    }

    @Test
    public void serverFailureLeavesCommitOutcomeUnknown() {
        client.failCommitWithServerError = true;
        InternalSession session = new InternalSession(client);
        Transaction transaction = session.beginTransaction();

        RuntimeException error = assertThrows(RuntimeException.class, transaction::commit);

        assertTrue(error.getMessage().contains("HTTP 500 internal error"));
        assertFalse(transaction.isOpen());
        assertThrows(IllegalStateException.class, session::beginTransaction);
    }

    @Test
    public void beginTransactionSendsRequestedMode() {
        InternalSession session = new InternalSession(client);

        session.beginTransaction(Transaction.Mode.READ_ONLY);

        assertEquals("{\"mode\":\"read_only\"}", client.lastRequestBody);
    }

    @Test
    public void closeRollsBackAndReleasesSession() {
        InternalSession session = new InternalSession(client);
        Transaction transaction = session.beginTransaction();

        transaction.close();

        assertEquals("/transactions/" + TRANSACTION_ID + "/rollback", client.lastPath);
        assertFalse(transaction.isOpen());
        assertTrue(session.beginTransaction().isOpen());
    }

    @Test
    public void transactionQueriesUseTpServiceRoute() {
        InternalSession session = new InternalSession(client);
        Transaction transaction = session.beginTransaction();

        transaction.run("RETURN 1");

        assertEquals("/transactions/" + TRANSACTION_ID + "/query", client.lastPath);
        transaction.rollback();
    }

    private static final class FakeClient extends Client {
        private boolean failCommit;
        private boolean busyCommit;
        private boolean expireCommit;
        private boolean rejectRollback;
        private boolean busyRollback;
        private boolean failCommitWithServerError;
        private String lastPath;
        private String lastRequestBody;

        private FakeClient() {
            super("http://localhost", Config.builder().build());
        }

        @Override
        public HttpResponse syncPost(HttpUrl url, byte[] request) throws IOException {
            String path = url.encodedPath();
            lastPath = path;
            lastRequestBody = new String(request, StandardCharsets.UTF_8);
            if ("/transactions".equals(path)) {
                return new HttpResponse(
                        201,
                        ("{\"transaction_id\":\"" + TRANSACTION_ID + "\"}")
                                .getBytes(StandardCharsets.UTF_8));
            }
            if (path.endsWith("/commit") && failCommit) {
                throw new IOException("response lost");
            }
            if (path.endsWith("/commit") && busyCommit) {
                return new HttpResponse(409, "transaction busy".getBytes(StandardCharsets.UTF_8));
            }
            if (path.endsWith("/commit") && expireCommit) {
                return new HttpResponse(
                        410, "transaction expired".getBytes(StandardCharsets.UTF_8));
            }
            if (path.endsWith("/commit") && failCommitWithServerError) {
                return new HttpResponse(500, "internal error".getBytes(StandardCharsets.UTF_8));
            }
            if (path.endsWith("/rollback") && rejectRollback) {
                return new HttpResponse(
                        410, "transaction expired".getBytes(StandardCharsets.UTF_8));
            }
            if (path.endsWith("/rollback") && busyRollback) {
                return new HttpResponse(409, "transaction busy".getBytes(StandardCharsets.UTF_8));
            }
            return new HttpResponse(200, new byte[0]);
        }
    }
}
