/*
 * MicroMulti.java
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;

import com.foundationdb.*;
import com.foundationdb.async.Function;
import com.foundationdb.async.Future;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;

public class MicroMulti {

	private static final FDB fdb;
	private static final Database db;
	private static final Subspace multi;
	private static final int N = 100;

	static {
		fdb = FDB.selectAPIVersion(300);
		db = fdb.open();
		multi = new Subspace(Tuple.from("M"));
	}

	// TODO These two methods (addHelp and getLong) are used by the methods
	// that are definitely in the book.
	private static void addHelp(TransactionContext tcx, final byte[] key, final long amount){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				ByteBuffer b = ByteBuffer.allocate(8);
				b.order(ByteOrder.LITTLE_ENDIAN);
				b.putLong(amount);

				tr.mutate(MutationType.ADD, key, b.array());

				return null;
			}
		});
	}

	private static long getLong(byte[] val){
		ByteBuffer b = ByteBuffer.allocate(8);
		b.order(ByteOrder.LITTLE_ENDIAN);
		b.put(val);
		return b.getLong(0);
	}

	// TODO These five methods are definitely in the recipe book
	// (add, subtract, get, getCounts, and isElement).
	public static void add(TransactionContext tcx, final String index,
							final Object value){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				addHelp(tr, multi.subspace(Tuple.from(index,value)).getKey(),1l);
				return null;
			}
		});
	}

	public static void subtract(TransactionContext tcx, final String index,
								final Object value){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				Future<byte[]> v = tr.get(multi.subspace(
										Tuple.from(index,value)).getKey());

				if(v.get() != null &&  getLong(v.get()) > 1l){
					addHelp(tr, multi.subspace(Tuple.from(index,value)).getKey(), -1l);
				} else {
					tr.clear(multi.subspace(Tuple.from(index,value)).getKey());
				}
				return null;
			}
		});
	}

	public static ArrayList<Object> get(TransactionContext tcx, final String index){
		return tcx.run(new Function<Transaction,ArrayList<Object> >() {
			public ArrayList<Object> apply(Transaction tr){
				ArrayList<Object> vals = new ArrayList<Object>();
				for(KeyValue kv : tr.getRange(multi.subspace(
									Tuple.from(index)).range())){
					vals.add(multi.unpack(kv.getKey()).get(1));
				}
				return vals;
			}
		});
	}

	public static HashMap<Object,Long> getCounts(TransactionContext tcx,
												final String index){
		return tcx.run(new Function<Transaction,HashMap<Object,Long> >() {
			public HashMap<Object,Long> apply(Transaction tr){
				HashMap<Object,Long> vals = new HashMap<Object,Long>();
				for(KeyValue kv : tr.getRange(multi.subspace(
										Tuple.from(index)).range())){
					vals.put(multi.unpack(kv.getKey()).get(1),
							getLong(kv.getValue()));
				}
				return vals;
			}
		});
	}

	public static boolean isElement(TransactionContext tcx, final String index,
								final Object value){
		return tcx.run(new Function<Transaction,Boolean>() {
			public Boolean apply(Transaction tr){
				return tr.get(multi.subspace(
						Tuple.from(index, value)).getKey()).get() != null;
			}
		});
	}

	public static void clearSubspace(TransactionContext tcx, final Subspace s){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				tr.clear(s.range());
				return null;
			}
		});
	}

	public static void main(String[] args) {
		clearSubspace(db, multi);
		for(int i = 0; i < N; i++){
			add(db, "foo", "bar");
			if(i % 100 == 0){
				System.out.println(i);
			}
		}

		for(int i = 0; i < N/10; i++){
			add(db,"foo","bear");
			add(db,"foo","boar");
			add(db,"fu","kung");
		}

		add(db,"fu","barre");
		subtract(db,"fu","barre");

		for(int i = 0; i < N/10; i++){
			subtract(db,"foo","bar");
			if(i % 100 == 0){
				System.out.println(i);
			}
		}

		System.out.println(isElement(db, "foo", "bar"));
		System.out.println(isElement(db, "foo", "bor"));
		System.out.println(isElement(db, "fu", "kung"));
		HashMap<Object,Long> map = getCounts(db,"foo");
		for(Entry<Object, Long> kv : map.entrySet()){
			System.out.println(kv);
		}
		map = getCounts(db,"fu");
		for(Entry<Object,Long> kv : map.entrySet()){
			System.out.println(kv);
		}
	}

}
