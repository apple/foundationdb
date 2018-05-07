/*
 * MicroRange.java
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

import java.util.ArrayList;

import com.foundationdb.*;
import com.foundationdb.async.Function;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;

public class MicroRange {

	private static final FDB fdb;
	private static final Database db;
	private static final Subspace range;
	private static final int LIMIT = 10;

	static {
		fdb = FDB.selectAPIVersion(300);
		db = fdb.open();
		range = new Subspace(Tuple.from("R"));
	}

	private static boolean haltingCondition(byte[] key, byte[] value){
		return true;
	}

	public static void stopRangeSoon(TransactionContext tcx, final Range r){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				// TODO This loop in recipe book.
				for(KeyValue kv : tr.getRange(r, ReadTransaction.ROW_LIMIT_UNLIMITED,
											  false, StreamingMode.SMALL)){
					if(haltingCondition(kv.getKey(), kv.getValue())){
						break;
					}
					System.out.println(Tuple.fromBytes(kv.getKey()).toString()
							+ " " + Tuple.fromBytes(kv.getValue()).toString());
				}

				return null;
			}
		});
	}

	// TODO This method in recipe book.
	public static void getRangeLimited(TransactionContext tcx, final KeySelector begin, final KeySelector end){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				boolean keysToCheck = true;
				ArrayList<Tuple> keysFound = new ArrayList<Tuple>();
				KeySelector n_begin = new KeySelector(begin.getKey(),true,begin.getOffset());
				while(keysToCheck){
					keysToCheck = false;
					for(KeyValue kv : tr.getRange(n_begin, end, LIMIT)){
						keysToCheck = true;
						Tuple t = Tuple.fromBytes(kv.getKey());
						if(keysFound.size() == 0
								|| !t.equals(keysFound.get(keysFound.size()-1))){
							keysFound.add(t);
						}
					}
					if(keysToCheck){
						n_begin = KeySelector.firstGreaterThan(keysFound.get(keysFound.size()-1).pack());
						ArrayList<Object> readableFound = new ArrayList<Object>();
						for(Tuple t : keysFound){
							readableFound.add(t.get(1));
						}
						System.out.println(readableFound);
						keysFound = new ArrayList<Tuple>();
					}
				}
				return null;
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

	public static void populate(TransactionContext tcx, final Subspace s){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				for(int i = 0; i < 10*LIMIT + LIMIT/2; i++){
					tr.set(s.pack(Tuple.from(i)), Tuple.from().pack());
				}
				return null;
			}
		});
	}

	public static void main(String[] args) {
		clearSubspace(db, range);
		populate(db, range);
		Range r = range.range();
		getRangeLimited(db, new KeySelector(r.begin,true,1), new KeySelector(r.end,true,1));
	}

}
