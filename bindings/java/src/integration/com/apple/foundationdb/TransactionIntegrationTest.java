/*
 * TransactionIntegrationTests.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.apple.foundationdb;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Miscellanenous tests for Java-bindings-specific implementation of
 * transactions
 */
@ExtendWith(RequiresDatabase.class)
public class TransactionIntegrationTest {
    private static final FDB fdb = FDB.selectAPIVersion(ApiVersion.LATEST);

    @Test
    public void testOperationsAfterCommit() throws Exception {
        try (Database db = fdb.open()) {
            for (int i = 0; i < 10; i++) {
                try (Transaction tr = db.createTransaction()) {
                    doTestOperationsAfterCommit(tr);
                }
            }
        }
    }

    @Test
    public void testOperationsAfterCommitInTenant() throws Exception {
        try (Database db = fdb.open()) {
            byte[] tenantName = "testOperationsAfterCommitInTenant".getBytes();
            TenantManagement.createTenant(db, tenantName).join();
            try (Tenant tenant = db.openTenant(tenantName)) {
                for (int i = 0; i < 10; i++) {
                    try (Transaction tr = tenant.createTransaction()) {
                        doTestOperationsAfterCommit(tr);
                    }
                }
            }
        }
    }

    private void doTestOperationsAfterCommit(Transaction tr) {
        tr.set("key1".getBytes(), "val1".getBytes());
        CompletableFuture<Void> commitFuture = tr.commit();

        // All operations after a submitted commit should fail
        expectUsedDuringCommitError(() -> {
            tr.get("key3".getBytes()).join();
        });
        // The set by itself has no effect
        tr.set("key2".getBytes(), "val2".getBytes());
        // But the second commit should fail too
        expectUsedDuringCommitError(() -> {
            tr.commit().join();
        });

        // The original commit should succeed
        commitFuture.join();

        // The behavior after completed commit should be the same
        expectUsedDuringCommitError(() -> {
            tr.get("key3".getBytes()).join();
        });
        tr.set("key2".getBytes(), "val2".getBytes());
        expectUsedDuringCommitError(() -> {
            tr.commit().join();
        });
    }

    private void expectUsedDuringCommitError(Runnable operation) {
        try {
            operation.run();
            Assertions.fail();
        } catch (CompletionException ce) {
            FDBException fdbEx = (FDBException) ce.getCause();
            Assertions.assertEquals(fdbEx.getCode(), 2017); // used_during_commit
        }
    }
}
