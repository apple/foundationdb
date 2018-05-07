/*
 * MicroIndexes.java
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

public class MicroIndexes {

	private static final FDB fdb;
	private static final Database db;
	private static final Subspace main;
	private static final Subspace index;

	static {
		fdb = FDB.selectAPIVersion(300);
		db = fdb.open();
		main = new Subspace(Tuple.from("user"));
		index = new Subspace(Tuple.from("zipcode_index"));
	}

	// TODO These three methods (setUser, getUser, and getUserIDsInRegion)
	// are all in the recipe book.
	public static void setUser(TransactionContext tcx, final String ID, final String name, final String zipcode){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				tr.set(main.pack(Tuple.from(ID,zipcode)), Tuple.from(name).pack());
				tr.set(index.pack(Tuple.from(zipcode,ID)), Tuple.from().pack());
				return null;
			}
		});
	}

	// Normal lookup.
	public static String getUser(TransactionContext tcx, final String ID){
		return tcx.run(new Function<Transaction,String>() {
			public String apply(Transaction tr){
				for(KeyValue kv : tr.getRange(main.subspace(Tuple.from(ID)).range(), 1)){
					// Return user with correct ID (if exists).
					return Tuple.fromBytes(kv.getValue()).getString(0);
				}
				return "";
			}
		});
	}

	// Index lookup.
	public static ArrayList<String> getUserIDsInRegion(TransactionContext tcx, final String zipcode){
		return tcx.run(new Function<Transaction,ArrayList<String>>() {
			public ArrayList<String> apply(Transaction tr){
				ArrayList<String> IDs = new ArrayList<String>();
				for(KeyValue kv : tr.getRange(index.subspace(Tuple.from(zipcode)).range())){
					IDs.add(index.unpack(kv.getKey()).getString(1));
				}
				return IDs;
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
		clearSubspace(db, main);
		clearSubspace(db, index);

		// Smoke test.
		setUser(db, "001", "Barack", "20500");
		setUser(db, "002", "Michelle", "20500");
		setUser(db, "003", "Sasha", "20500");
		setUser(db, "004", "Malia", "20500");
		setUser(db, "005", "Bo", "20500");
		setUser(db, "101", "Elizabeth", "SW1A 1AA");
		setUser(db, "102", "Philip", "SW1A 1AA");

		String[] IDs = {"001","002","003","004","005","101","102"};
		String[] zips = {"20500","SW1A 1AA"};

		for(String id : IDs){
			System.out.println(getUser(db,id));
		}

		for(String zip : zips){
			System.out.println(getUserIDsInRegion(db, zip));
		}
	}

}
