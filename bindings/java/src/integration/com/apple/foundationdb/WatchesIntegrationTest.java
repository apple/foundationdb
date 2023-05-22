/*
 * WatchesIntegrationTest.java
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests for working with FDB futures
 */
@ExtendWith(RequiresDatabase.class)
class WatchesIntegrationTest {
  private static final FDB fdb = FDB.selectAPIVersion(ApiVersion.LATEST);

  private static final int WATCH_TIMEOUT_SEC = 10;

  static class DirectExecutor implements Executor {
    @Override
    public void execute(Runnable command) {
      command.run();
    }
  }

  @Test
  @Tag("SupportsExternalClient")
  public void testWatchesWithinLimit() throws Exception {
    final String prefix = "eee";
    final int numKeys = 10;
    final int watchLimit = 10;
    try (Database db = fdb.open()) {
      db.options().setMaxWatches(watchLimit);
      ensureConnected(db);
      setTestKeys(db, prefix, numKeys, "oldVal");
      List<CompletableFuture<Void>> futureList = new ArrayList<CompletableFuture<Void>>();
      for (int i = 0; i < numKeys; i++) {
        final int idx = i;
        db.run(tr -> {
          futureList.add(createTestWatch(tr, prefix, idx));
          return null;
        });
      }
      setTestKeys(db, prefix, numKeys, "newVal");
      for (CompletableFuture<Void> future : futureList) {
        future.orTimeout(WATCH_TIMEOUT_SEC, TimeUnit.SECONDS).join();
      }
    }
  }

  @Test
  @Tag("SupportsExternalClient")
  public void testWatchesWithinLimitInSingleTx() throws Exception {
    final String prefix = "aaa";
    final int numKeys = 10;
    final int watchLimit = 10;
    try (Database db = fdb.open()) {
      db.options().setMaxWatches(watchLimit);
      ensureConnected(db);
      setTestKeys(db, prefix, numKeys, "oldVal");
      List<CompletableFuture<Void>> futureList = new ArrayList<CompletableFuture<Void>>();
      db.run(tr -> {
        for (int i = 0; i < numKeys; i++) {
          futureList.add(createTestWatch(tr, prefix, i));
        }
        return null;
      });
      setTestKeys(db, prefix, numKeys, "newVal");
      for (CompletableFuture<Void> future : futureList) {
        future.orTimeout(WATCH_TIMEOUT_SEC, TimeUnit.SECONDS).join();
      }
    }
  }

  @Test
  @Tag("SupportsExternalClient")
  public void testWatchesOverTheLimit() throws Exception {
    final String prefix = "bbb";
    final int numKeys = 11;
    final int watchLimit = 10;
    try (Database db = fdb.open()) {
      db.options().setMaxWatches(watchLimit);
      ensureConnected(db);
      setTestKeys(db, prefix, numKeys, "oldVal");
      List<CompletableFuture<Void>> futureList = new ArrayList<CompletableFuture<Void>>();
      for (int i = 0; i < numKeys; i++) {
        final int idx = i;
        db.run(tr -> {
          futureList.add(createTestWatch(tr, prefix, idx));
          return null;
        });
      }
      setTestKeys(db, prefix, numKeys, "newVal");
      try {
        for (CompletableFuture<Void> future : futureList) {
          future.orTimeout(1, TimeUnit.SECONDS).join();
        }
        Assertions.fail("Watch limit not imposed");
      } catch (CompletionException e) {
        Assertions.assertTrue(e.getCause() instanceof FDBException);
        Assertions.assertEquals(1032, ((FDBException) e.getCause()).getCode());
      }
    }
  }

  @Test
  @Tag("SupportsExternalClient")
  public void testWatchesOverTheLimitInSingleTx() throws Exception {
    final String prefix = "ccc";
    final int numKeys = 11;
    final int watchLimit = 10;
    try (Database db = fdb.open()) {
      db.options().setMaxWatches(watchLimit);
      ensureConnected(db);
      List<CompletableFuture<Void>> futureList = new ArrayList<CompletableFuture<Void>>();
      setTestKeys(db, prefix, numKeys, "oldVal");
      db.run(tr -> {
        for (int i = 0; i < numKeys; i++) {
          futureList.add(createTestWatch(tr, prefix, i));
        }
        return null;
      });
      setTestKeys(db, prefix, numKeys, "newVal");
      try {
        for (CompletableFuture<Void> future : futureList) {
          future.orTimeout(1, TimeUnit.SECONDS).join();
        }
        Assertions.fail("Watch limit not imposed");
      } catch (CompletionException e) {
        Assertions.assertTrue(e.getCause() instanceof FDBException);
        Assertions.assertEquals(1032, ((FDBException) e.getCause()).getCode());
      }
    }
  }

  @Test
  @Tag("SupportsExternalClient")
  public void testWatchCleanupOnCancel() throws Exception {
    final String prefix = "ddd";
    final int numKeys = 100;
    final int watchesToCancel = 90;
    final int watchLimit = 10;
    try (Database db = fdb.open()) {
      db.options().setMaxWatches(watchLimit);
      ensureConnected(db);
      setTestKeys(db, prefix, numKeys, "oldVal");
      List<CompletableFuture<Void>> futureList = new ArrayList<CompletableFuture<Void>>();
      for (int i = 0; i < numKeys; i++) {
        final int idx = i;
        db.run(tr -> {
          CompletableFuture<Void> result = createTestWatch(tr, prefix, idx);
          futureList.add(result);
          if (idx < watchesToCancel) {
            result.cancel(true);
          }
          return null;
        });
      }
      setTestKeys(db, prefix, numKeys, "newVal");
      int idx = 0;
      for (CompletableFuture<Void> future : futureList) {
        if (idx < watchesToCancel) {
          try {
            future.orTimeout(1, TimeUnit.SECONDS).join();
            Assertions.fail("Expecting cancellation exception");
          } catch (CancellationException e) {
          }
        } else {
          future.orTimeout(WATCH_TIMEOUT_SEC, TimeUnit.SECONDS).join();
        }
        idx++;
      }
    }
  }

  @Test
  @Tag("SupportsExternalClient")
  public void testWatchCleanupInSingleTxOnCancel() throws Exception {
    final String prefix = "ddd";
    final int numKeys = 100;
    final int watchesToCancel = 90;
    final int watchLimit = 10;
    try (Database db = fdb.open()) {
      db.options().setMaxWatches(watchLimit);
      ensureConnected(db);
      setTestKeys(db, prefix, numKeys, "oldVal");
      List<CompletableFuture<Void>> futureList = new ArrayList<CompletableFuture<Void>>();

      db.run(tr -> {
        for (int i = 0; i < numKeys; i++) {
          CompletableFuture<Void> result = createTestWatch(tr, prefix, i);
          futureList.add(result);
          if (i < watchesToCancel) {
            result.cancel(true);
          }
        }
        return null;
      });

      setTestKeys(db, prefix, numKeys, "newVal");
      int idx = 0;
      for (CompletableFuture<Void> future : futureList) {
        if (idx < watchesToCancel) {
          try {
            future.orTimeout(1, TimeUnit.SECONDS).join();
            Assertions.fail("Expecting cancellation exception");
          } catch (CancellationException e) {
          }
        } else {
          future.orTimeout(WATCH_TIMEOUT_SEC, TimeUnit.SECONDS).join();
        }
        idx++;
      }
    }
  }

  @Test
  @Tag("SupportsExternalClient")
  public void testWatchCleanupOnClose() throws Exception {
    final String prefix = "ddd";
    final int numKeys = 100;
    final int watchesToClose = 90;
    final int watchLimit = 10;
    try (Database db = fdb.open()) {
      db.options().setMaxWatches(watchLimit);
      ensureConnected(db);
      setTestKeys(db, prefix, numKeys, "oldVal");
      List<CompletableFuture<Void>> futureList = new ArrayList<CompletableFuture<Void>>();
      for (int i = 0; i < numKeys; i++) {
        final int idx = i;
        db.run(tr -> {
          CompletableFuture<Void> result = createTestWatch(tr, prefix, idx);
          if (idx >= watchesToClose) {
            futureList.add(result);
          } else {
            ((NativeFuture<Void>)result).close();
          }
          return null;
        });
      }
      System.gc();
      setTestKeys(db, prefix, numKeys, "newVal");
      for (CompletableFuture<Void> future : futureList) {
        future.orTimeout(WATCH_TIMEOUT_SEC, TimeUnit.SECONDS).join();
      }
    }
  }

  private void ensureConnected(Database db) throws Exception {
    // Run one transaction succesfully to ensure we established connection
    db.run(tr -> {
      tr.getReadVersion().join();
      return null;
    });
  }

  private void setTestKeys(Database db, String prefix, int numberOfKeys, String value) throws Exception {
    db.run(tr -> {
      for (int i = 0; i < numberOfKeys; i++) {
        tr.set(String.format("%s%d", prefix, i).getBytes(), value.getBytes());
      }
      return null;
    });
  }

  private CompletableFuture<Void> createTestWatch(Transaction tr, String prefix, int idx) {
    return tr.watch(String.format("%s%d", prefix, idx).getBytes());
  }

}