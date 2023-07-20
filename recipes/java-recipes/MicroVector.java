/*
 * MicroVector.java
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

public class MicroVector {

	private static final FDB fdb;
	private static final Database db;
	private static final Subspace vector;

	static {
		fdb = FDB.selectAPIVersion(300);
		db = fdb.open();
		vector = new Subspace(Tuple.from("V"));
	}

	// TODO These two methods--the get and set methods--are in the recipes.
	public static Object get(TransactionContext tcx, final long index){
		return tcx.run(new Function<Transaction,Object>() {
			public Object apply(Transaction tr){
				return Tuple.fromBytes(tr.get(vector.pack(
								Tuple.from(index))).get()).get(0);
			}
		});
	}

	public static void set(TransactionContext tcx, final long index, final Object value){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				tr.set(vector.pack(Tuple.from(index)), Tuple.from(value).pack());
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

	public static void main(String[] args) {
		clearSubspace(db, vector);

		// Smoke test.
		String[] vals = {"Alice", "Bob", "Carol", "David", "Eve"};

		for(int i = 0; i < vals.length; i++){
			set(db, i, vals[i]);
		}

		for(int i = 0; i < vals.length; i++){
			System.out.println(get(db, i));
		}

		set(db, 3, "Dylan");

		for(int i = 0; i < vals.length; i++){
			System.out.println(get(db,i));
		}

	}

}
