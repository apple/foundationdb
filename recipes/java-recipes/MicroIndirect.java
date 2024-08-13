/*
 * MicroIndirect.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

import com.foundationdb.*;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.directory.DirectoryLayer;
import com.foundationdb.directory.DirectorySubspace;
import com.foundationdb.directory.PathUtil;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;

public class MicroIndirect {

	private static final FDB fdb;

	static {
		fdb = FDB.selectAPIVersion(300);
	}

	// TODO This is the code for the context manager that appears at
	// the end of the recipe book.
	public static class Workspace {
		private final Database db;
		private final DirectorySubspace dir;

		public Workspace(DirectorySubspace directory, Database db){
			this.dir = directory;
			this.db = db;
		}

		public Future<DirectorySubspace> getCurrent() {
			return dir.createOrOpen(this.db, PathUtil.from("current"));
		}

		public Future<DirectorySubspace> getNew() {
			return dir.createOrOpen(this.db, PathUtil.from("new"));
		}

		public Future<DirectorySubspace> replaceWithNew() {
			return this.db.runAsync(new Function<Transaction,Future<DirectorySubspace>>() {
				public Future<DirectorySubspace> apply(final Transaction tr){
					return dir.remove(tr, PathUtil.from("current")) // Clear the old current.
							.flatMap(new Function<Void,Future<DirectorySubspace>>() {
								public Future<DirectorySubspace> apply(Void arg0) {
									// Replace the old directory with the new one.
									return dir.move(tr, PathUtil.from("new"), PathUtil.from("current"));
								}
							});
				}
			});
		}
	}

	public static void clearSubspace(TransactionContext tcx, final Subspace s){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				tr.clear(s.range());
				return null;
			}
		});
	}

	private static String tupleRep(Tuple t){
		StringBuilder sb = new StringBuilder();
		sb.append('(');
		for(Object o : t){
			sb.append(o.toString() + ",");
		}
		sb.append(')');
		return sb.toString();
	}

	public static void printSubspace(TransactionContext tcx, final Subspace s){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr) {
				for(KeyValue kv : tr.getRange(s.range())){
					System.out.println(tupleRep(Tuple.fromBytes(kv.getKey()))
							+ " "
							+ Tuple.fromBytes(kv.getValue()).get(0)
								.toString());
				}

				return null;
			}
		});
	}

	public static void main(String[] args) {
		// TODO These four lines are the equivalent of the initialization
		// steps in the recipe book.
		Database db = fdb.open();
		Future<DirectorySubspace> workingDir = DirectoryLayer.getDefault()
				.createOrOpen(db, PathUtil.from("working"));
		Workspace workspace = new Workspace(workingDir.get(), db);
		final DirectorySubspace current = workspace.getCurrent().get();

		clearSubspace(db, current);
		db.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				tr.set(current.pack(Tuple.from(1)), Tuple.from("a").pack());
				tr.set(current.pack(Tuple.from(2)), Tuple.from("b").pack());
				return null;
			}
		});
		System.out.println("Contents:");
		printSubspace(db, workspace.getCurrent().get());

		// TODO This is the equivalent of the "with" block in the
		// python version of the book.
		final DirectorySubspace newspace = workspace.getNew().get();
		try{
			clearSubspace(db, newspace);
			db.run(new Function<Transaction,Void>() {
				public Void apply(Transaction tr){
					tr.set(newspace.pack(Tuple.from(3)),Tuple.from("c").pack());
					tr.set(newspace.pack(Tuple.from(4)), Tuple.from("d").pack());
					return null;
				}
			});
		} finally {
			// Asynchronous operation--wait until result is reached.
			workspace.replaceWithNew().blockUntilReady();
		}

		System.out.println("Contents:");
		printSubspace(db, workspace.getCurrent().get());
	}

}
