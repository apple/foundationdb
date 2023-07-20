/*
 * MicroQueue.java
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

import com.foundationdb.*;
import com.foundationdb.async.Function;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;

import java.util.Random;

public class MicroQueue {

	private static final FDB fdb;
	private static final Database db;
	private static final Subspace queue;
	private static final Random randno;

	static{
		fdb = FDB.selectAPIVersion(300);
		db = fdb.open();
		queue = new Subspace(Tuple.from("Q"));
		randno = new Random();
	}

	// TODO The next four elements are in the recipe book:
	// dequeue, enqueue, firstItem, and lastIndex.

	// Remove the top element from the queue.
	public static Object dequeue(TransactionContext tcx){
		// Remove from the top of the queue.
		return tcx.run(new Function<Transaction,Void>(){
			public Void apply(Transaction tr){
				final KeyValue item = firstItem(tr);
				if(item == null){
					return null;
				}

				tr.clear(item.getKey());
				// Return the old value.
				return Tuple.fromBytes(item.getValue()).get(0);
			}
		});

	}

	// Add an element to the queue.
	public static void enqueue(TransactionContext tcx, final Object value){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				byte[] rands = new byte[20];
				randno.nextBytes(rands); // Create random seed to avoid conflicts.
				tr.set(queue.subspace(Tuple.from(lastIndex(tr)+1, rands)).pack(),
						Tuple.from(value).pack());

				return null;
			}
		});
	}

	// Get the top element of the queue.
	private static KeyValue firstItem(TransactionContext tcx){
		return tcx.run(new Function<Transaction, KeyValue>() {
			public KeyValue apply(Transaction tr){
				for(KeyValue kv : tr.getRange(queue.range(), 1)){
					return kv;
				}

				return null; // Empty queue. Should never be reached.
			}
		});
	}

	// Get the last index in the queue.
	private static long lastIndex(TransactionContext tcx){
		return tcx.run(new Function<Transaction, Long>() {
			public Long apply(Transaction tr){
				for(KeyValue kv : tr.snapshot().getRange(queue.range(), 1, true)){
					return (long)queue.unpack(kv.getKey()).get(0);
				}
				return 0l;
			}
		});
	}

	// Clears subspaces of a database.
	public static void clearSubspace(TransactionContext tcx, final Subspace s){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				tr.clear(Range.startsWith(s.getKey()));
				return null;
			}
		});
	}

	public static void main(String[] args){
		clearSubspace(db, queue);
		String[] line = {"Alice", "Bob", "Carol", "Dave", "Eve", "Frank","George",
						 "Harry", "Ian","Jack","Liz","Mary","Nathan"};

		for(int i = 0; i < line.length; i++){
			enqueue(db, line[i]);
		}

		Object o;
		while((o = dequeue(db)) != null){
			System.out.println(o);
		}
	}
}
