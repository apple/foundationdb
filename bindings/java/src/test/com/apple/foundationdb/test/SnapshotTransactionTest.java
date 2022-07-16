/*
 * SnapshotTransactionTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2018 Apple Inc. and the FoundationDB project authors
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

package com.apple.foundationdb.test;

import java.util.UUID;
import java.util.concurrent.CompletionException;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.ReadTransaction;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

/**
 * Some tests regarding conflict ranges to make sure they do what we expect.
 */
public class SnapshotTransactionTest {
	private static final int CONFLICT_CODE = 1020;
	private static final Subspace SUBSPACE = new Subspace(Tuple.from("test", "conflict_ranges"));

	public static void main(String[] args) {
		FDB fdb = FDB.selectAPIVersion(720);
		try(Database db = fdb.open()) {
			snapshotReadShouldNotConflict(db);
			snapshotShouldNotAddConflictRange(db);
			snapshotOnSnapshot(db);
		}
	}

	// Adding a random write conflict key makes it so the transaction conflicts are actually resolved.
	public static void addUUIDConflicts(Transaction... trs) {
		for(Transaction tr : trs) {
			tr.options().setTimeout(1000);
			tr.getReadVersion().join();
			byte[] key = SUBSPACE.pack(Tuple.from("uuids", UUID.randomUUID()));
			tr.addReadConflictKey(key);
			tr.addWriteConflictKey(key);
		}
	}

	public static <E extends Exception> void validateConflict(E e) throws E {
		FDBException fdbE = null;
		Throwable current = e;
		while(current != null && fdbE == null) {
			if(current instanceof FDBException) {
				fdbE = (FDBException)current;
			}
			else {
				current = current.getCause();
			}
		}
		if(fdbE == null) {
			System.err.println("Error was not caused by FDBException");
			throw e;
		}
		else {
			int errorCode = fdbE.getCode();
			if(errorCode != CONFLICT_CODE) {
				System.err.println("FDB error was not caused by a transaction conflict");
				throw e;
			}
		}
	}

	public static void snapshotReadShouldNotConflict(Database db) {
		try(Transaction tr1 = db.createTransaction(); Transaction tr2 = db.createTransaction(); Transaction tr3 = db.createTransaction()) {
			addUUIDConflicts(tr1, tr2, tr3);

			// Verify reading a *range* causes a conflict
			tr1.addWriteConflictKey(SUBSPACE.pack(Tuple.from("foo", 0L)));
			tr2.snapshot().getRange(SUBSPACE.range(Tuple.from("foo"))).asList().join();
			tr3.getRange(SUBSPACE.range(Tuple.from("foo"))).asList().join();

			// Two successful commits
			tr1.commit().join();
			tr2.commit().join();

			// Read from tr3 should conflict with update from tr1.
			try {
				tr3.commit().join();
				throw new RuntimeException("tr3 did not conflict");
			} catch(CompletionException e) {
				validateConflict(e);
			}
		}
		try(Transaction tr1 = db.createTransaction(); Transaction tr2 = db.createTransaction(); Transaction tr3 = db.createTransaction()) {
			addUUIDConflicts(tr1, tr2, tr3);

			// Verify reading a *key* causes a conflict
			byte[] key = SUBSPACE.pack(Tuple.from("foo", 1066L));
			tr1.addWriteConflictKey(key);
			tr2.snapshot().get(key);
			tr3.get(key).join();

			tr1.commit().join();
			tr2.commit().join();

			try {
				tr3.commit().join();
				throw new RuntimeException("tr3 did not conflict");
			}
			catch(CompletionException e) {
				validateConflict(e);
			}
		}
	}

	public static void snapshotShouldNotAddConflictRange(Database db) {
		try(Transaction tr1 = db.createTransaction(); Transaction tr2 = db.createTransaction(); Transaction tr3 = db.createTransaction()) {
			addUUIDConflicts(tr1, tr2, tr3);

			// Verify adding a read conflict *range* causes a conflict.
			Subspace fooSubspace = SUBSPACE.subspace(Tuple.from("foo"));
			tr1.addWriteConflictKey(fooSubspace.pack(Tuple.from(0L)));
			byte[] beginKey = fooSubspace.range().begin;
			byte[] endKey = fooSubspace.range().end;
			if(tr2.snapshot().addReadConflictRangeIfNotSnapshot(beginKey, endKey)) {
				throw new RuntimeException("snapshot read said it added a conflict range");
			}
			if(!tr3.addReadConflictRangeIfNotSnapshot(beginKey, endKey)) {
				throw new RuntimeException("non-snapshot read said it did not add a conflict range");
			}

			// Two successful commits
			tr1.commit().join();
			tr2.commit().join();

			// Read from tr3 should conflict with update from tr1.
			try {
				tr3.commit().join();
				throw new RuntimeException("tr3 did not conflict");
			}
			catch(CompletionException e) {
				validateConflict(e);
			}
		}
		try(Transaction tr1 = db.createTransaction(); Transaction tr2 = db.createTransaction(); Transaction tr3 = db.createTransaction()) {
			addUUIDConflicts(tr1, tr2, tr3);

			// Verify adding a read conflict *key* causes a conflict.
			byte[] key = SUBSPACE.pack(Tuple.from("foo", 1066L));
			tr1.addWriteConflictKey(key);
			if(tr2.snapshot().addReadConflictKeyIfNotSnapshot(key)) {
				throw new RuntimeException("snapshot read said it added a conflict range");
			}
			if(!tr3.addReadConflictKeyIfNotSnapshot(key)) {
				throw new RuntimeException("non-snapshot read said it did not add a conflict range");
			}

			// Two successful commits
			tr1.commit().join();
			tr2.commit().join();

			// Read from tr3 should conflict with update from tr1.
			try {
				tr3.commit().join();
				throw new RuntimeException("tr3 did not conflict");
			}
			catch(CompletionException e) {
				validateConflict(e);
			}
		}
	}

	private static void snapshotOnSnapshot(Database db) {
		try(Transaction tr = db.createTransaction()) {
			if(tr.isSnapshot()) {
				throw new RuntimeException("new transaction is a snapshot transaction");
			}
			ReadTransaction snapshotTr = tr.snapshot();
			if(!snapshotTr.isSnapshot()) {
				throw new RuntimeException("snapshot transaction is not a snapshot transaction");
			}
			if(snapshotTr == tr) {
				throw new RuntimeException("snapshot and regular transaction are pointer-equal");
			}
			ReadTransaction snapshotSnapshotTr = snapshotTr.snapshot();
			if(!snapshotSnapshotTr.isSnapshot()) {
				throw new RuntimeException("snapshot transaction is not a snapshot transaction");
			}
			if(snapshotSnapshotTr != snapshotTr) {
				throw new RuntimeException("calling snapshot on a snapshot transaction produced a different transaction");
			}
		}
	}

	private SnapshotTransactionTest() {}
}

