/*
 * FutureIntegrationTest.java
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
import java.util.concurrent.Executor;
import java.util.function.Function;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for working with FDB futures
 */
@ExtendWith(RequiresDatabase.class)
class FutureIntegrationTest {
  private static final FDB fdb = FDB.selectAPIVersion(ApiVersion.LATEST);

  static class DirectExecutor implements Executor {
    @Override
    public void execute(Runnable command) {
      command.run();
    }
  }

  @Test
  @Tag("SupportsExternalClient")
  public void testCancelFuture() throws Exception {
    testTransaction(tr -> {
      CompletableFuture<byte[]> result = tr.get("hello".getBytes());
      result.cancel(true);
      return null;
    });
  }

  @Test
  @Tag("SupportsExternalClient")
  public void testCancelWithCallbackSet() throws Exception {
    testTransaction(tr -> {
      CompletableFuture<byte[]> result = tr.get("hello".getBytes());
      result.thenAcceptAsync(val -> {
        Assertions.assertNull(val);
        CompletableFuture<byte[]> res2 = tr.get("world".getBytes());
        res2.join();
      });
      result.cancel(true);
      return null;
    });
  }

  @Test
  @Tag("SupportsExternalClient")
  public void testSetCallbackAfterJoin() throws Exception {
    testTransaction(tr -> {
      CompletableFuture<byte[]> result = tr.get("hello".getBytes());
      result.join();
      result.thenAcceptAsync(val -> {
        Assertions.assertNull(val);
        CompletableFuture<byte[]> res2 = tr.get("world".getBytes());
        res2.join();
      });
      return null;
    });
  }

  @Test
  @Tag("SupportsExternalClient")
  public void testCancelFromCallback() throws Exception {
    testTransaction(tr -> {
      CompletableFuture<byte[]> result = tr.get("hello".getBytes());
      result.thenAcceptAsync(val -> {
        Assertions.assertNull(val);
        CompletableFuture<byte[]> res2 = tr.get("world".getBytes());
        result.cancel(true);
        res2.cancel(true);
      });
      return null;
    });
  }

  private <T> void testTransaction(Function<? super Transaction, T> retryable) {
    // Test with a thread pool for callbacks
    try (Database db = fdb.open()) {
      for (int i = 0; i < 10; i++) {
        db.run(retryable);
      }
    }
    // Test with a direct executor for callbacks
    try (Database db = fdb.open(null, new DirectExecutor())) {
      for (int i = 0; i < 10; i++) {
        db.run(retryable);
      }
    }
  }

}
