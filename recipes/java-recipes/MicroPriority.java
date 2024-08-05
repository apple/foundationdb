/*
 * MicroPriority.java
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
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;

import java.util.Random;

public class MicroPriority {

	private static final FDB fdb;
	private static final Database db;
	private static final Subspace pq;
	private static final Random randno;

	static{
		fdb = FDB.selectAPIVersion(300);
		db = fdb.open();
		pq = new Subspace(Tuple.from("P"));

		randno = new Random();
	}

	// TODO The next six methods are all in the recipe book:
	// push, nextCount, pop (x2), and peek (x2).
	public static void push(TransactionContext tcx, final Object value, final int priority){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				byte[] rands = new byte[20];
				randno.nextBytes(rands);
				tr.set(pq.subspace(Tuple.from(priority, nextCount(tr,priority),rands)).pack(),
						Tuple.from(value).pack());
				return null;
			}
		});
	}

	private static long nextCount(TransactionContext tcx, final int priority){
		return tcx.run(new Function<Transaction,Long>() {
			public Long apply(Transaction tr){
				for(KeyValue kv : tr.snapshot().getRange(pq.subspace(Tuple.from(priority)).range(),1,true)){
					return 1l + (long)pq.subspace(Tuple.from(priority)).unpack(kv.getKey()).get(0);
				}

				return 0l; // None previously with this priority.
			}
		});
	}

	// Pop--assumes min priority queue..
	public static Object pop(TransactionContext tcx){
		return pop(tcx,false);
	}

	// Pop--allows for either max or min priority queue.
	public static Object pop(TransactionContext tcx, final boolean max){
		return tcx.run(new Function<Transaction,Object>() {
			public Object apply(Transaction tr){
				for(KeyValue kv : tr.getRange(pq.range(), 1, max)){
					tr.clear(kv.getKey());
					return Tuple.fromBytes(kv.getValue()).get(0);
				}

				return null;
			}
		});
	}

	// Peek--assumes min priority queue.
	public static Object peek(TransactionContext tcx){
		return peek(tcx,false);
	}

	// Peek--allows for either max or min priority queue.
	public static Object peek(TransactionContext tcx, final boolean max){
		return tcx.run(new Function<Transaction,Object>() {
			public Object apply(Transaction tr){
				Range r = pq.range();
				for(KeyValue kv : tr.getRange(r.begin, r.end, 1, max)){
					return Tuple.fromBytes(kv.getValue()).get(0);
				}

				return null;
			}
		});
	}

	public static void clearSubspace(TransactionContext tcx, final Subspace s){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				tr.clear(Range.startsWith(s.getKey()));
				return null;
			}
		});
	}

	public static void smokeTest(){
		System.out.print("Peek none: ");
		System.out.println(peek(db));

		push(db, "a", 1);
		push(db, "b", 5);
		push(db, "c", 2);
		push(db, "d", 4);
		push(db, "e", 3);

		System.out.println("Peek in min order.");
		System.out.println(peek(db));
		System.out.println(peek(db));

		System.out.println("Pop in min order.");
		for(int i = 0; i < 5; i++) System.out.println(pop(db));

		System.out.print("Peek none: ");
		System.out.println(peek(db,true));

		push(db, "a1", 1);
		push(db, "a2", 1);
		push(db, "a3", 1);
		push(db, "a4", 1);
		push(db, "b", 5);
		push(db, "c", 2);
		push(db, "d", 4);
		push(db, "e", 3);

		System.out.println("Peek in max order.");
		System.out.println(peek(db, true));
		System.out.println(peek(db, true));

		System.out.println("Pop in max order.");
		for(int i = 0; i < 9; i++) System.out.println(pop(db, true));
	}

	public static void main(String[] args){
		clearSubspace(db, pq);
		smokeTest();
	}
}
