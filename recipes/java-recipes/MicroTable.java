/*
 * MicroTable.java
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

import java.util.Map;
import java.util.TreeMap;

import com.foundationdb.*;
import com.foundationdb.async.Function;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;

public class MicroTable {

	private static final FDB fdb;
	private static final Database db;
	private static final Subspace table;
	private static final Subspace rowIndex;
	private static final Subspace colIndex;

	static {
		fdb = FDB.selectAPIVersion(300);
		db = fdb.open();
		table = new Subspace(Tuple.from("T"));
		rowIndex = table.subspace(Tuple.from("R"));
		colIndex = table.subspace(Tuple.from("C"));
	}

	// TODO The recipe book definitely includes the following things:
	// the pack and unpack helper methods; the setCell and getCell
	// methods; and the setRow, getRow, and getCol methods. The setCol
	// method is not included in the python version, but it may be
	// worth including it in the java version--it may not.

	// Packing and unpacking helper functions.
	private static byte[] pack(Object value){
		return Tuple.from(value).pack();
	}

	private static Object unpack(byte[] value){
		return Tuple.fromBytes(value).get(0);
	}

	public static void setCell(TransactionContext tcx, final String row,
								final String column, final Object value){
		tcx.run(new Function<Transaction, Void>() {
			public Void apply(Transaction tr){
				tr.set(rowIndex.subspace(Tuple.from(row, column)).getKey(),
						pack(value));
				tr.set(colIndex.subspace(Tuple.from(column,row)).getKey(),
						pack(value));

				return null;
			}
		});
	}

	public static Object getCell(TransactionContext tcx, final String row,
								final String column){
		return tcx.run(new Function<Transaction, Object>() {
			public Object apply(Transaction tr){
				return unpack(tr.get(rowIndex.subspace(
						Tuple.from(row,column)).getKey()).get());
			}
		});
	}

	public static void setRow(TransactionContext tcx, final String row,
								final Map<String,Object> cols){
		tcx.run(new Function<Transaction, Void>() {
			public Void apply(Transaction tr){
				tr.clear(rowIndex.subspace(Tuple.from(row)).range());

				for(Map.Entry<String,Object> cv : cols.entrySet()){
					setCell(tr, row, cv.getKey(), cv.getValue());
				}
				return null;
			}
		});
	}

	public static void setColumn(TransactionContext tcx, final String column,
									final Map<String,Object> rows){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				tr.clear(colIndex.subspace(Tuple.from(column)).range());
				for(Map.Entry<String,Object> rv : rows.entrySet()){
					setCell(tr, rv.getKey(), column, rv.getValue());
				}
				return null;
			}
		});
	}

	public static TreeMap<String,Object> getRow(TransactionContext tcx,
												final String row){
		return tcx.run(new Function<Transaction,TreeMap<String,Object> >() {
			public TreeMap<String,Object> apply(Transaction tr){
				TreeMap<String,Object> cols = new TreeMap<String,Object>();

				for(KeyValue kv : tr.getRange(
						rowIndex.subspace(Tuple.from(row)).range())){
					cols.put(rowIndex.unpack(kv.getKey()).getString(1),
							unpack(kv.getValue()));
				}

				return cols;
			}
		});
	}


	public static TreeMap<String,Object> getColumn(TransactionContext tcx,
												final String column){
		return tcx.run(new Function<Transaction,TreeMap<String,Object> >() {
			public TreeMap<String,Object> apply(Transaction tr){
				TreeMap<String,Object> rows = new TreeMap<String,Object>();

				for(KeyValue kv : tr.getRange(
						colIndex.subspace(Tuple.from(column)).range())){
					rows.put(colIndex.unpack(kv.getKey()).getString(1),
							unpack(kv.getValue()));
				}

				return rows;
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
		/*
		 * Start with a simple table, and then run some operations on it.
		 * Row key is seat number, column values are fields.
		 * 		Name				President	Law school
		 * 0	John G. Roberts		Bush (43)	Harvard
		 * 1	Sonia Sotomayor		Obama		Yale
		 * 2	Stephen G. Breyer	Clinton		Harvard
		 * 3	Elena Kagan			Obama		Harvard
		 * 4	Anthony M. Kennedy	Reagan		Harvard
		 * 6	Ruth Bader Ginsburg	Clinton		Columbia
		 * 8	Samuel A. Alito		Bush(43)	Yale
		 * 9	Antonin Scalia		Reagan		Harvard
		 * 10	Clarence Thomas		Bush (41)	Yale
		 *
		 * During the test, we make one row change (replacing Kagan with
		 * her predcessor, John Paul Stevens) and one column change
		 * (replacing the short forms of the Presidential names with
		 * longer versions).
		 */

		// Populate database.
		setCell(db, "0", "Name", "John G. Roberts");
		setCell(db, "0", "President", "Bush (43)");
		setCell(db, "0", "Law school", "Harvard");
		setCell(db, "1", "Name", "Sonia Sotomayor");
		setCell(db, "1", "President", "Obama");
		setCell(db, "1", "Law school", "Yale");
		setCell(db, "2", "Name", "Stephen G. Breyer");
		setCell(db, "2", "President", "Clinton");
		setCell(db, "2", "Law school", "Harvard");
		setCell(db, "3", "Name", "Elena Kagan");
		setCell(db, "3", "President", "Obama");
		setCell(db, "3", "Law school", "Harvard");
		setCell(db, "4", "Name", "Anthony M. Kennedy");
		setCell(db, "4", "President", "Reagan");
		setCell(db, "4", "Law school", "Harvard");
		setCell(db, "6", "Name", "Ruth Bader Ginsburg");
		setCell(db, "6", "President", "Clinton");
		setCell(db, "6", "Law school", "Columbia");
		setCell(db, "8", "Name", "Samuel A. Alito");
		setCell(db, "8", "President", "Bush (43)");
		setCell(db, "8", "Law school", "Yale");
		setCell(db, "9", "Name", "Antonin Scalia");
		setCell(db, "9", "President", "Reagan");
		setCell(db, "9", "Law school", "Harvard");
		setCell(db, "10", "Name", "Clarence Thomas");
		setCell(db, "10", "President", "Bush (41)");
		setCell(db, "10", "Law school", "Yale");

		String[] rowlabs = {"0","1","2","3","4","6","8","9","10"};
		String[] collabs = {"Name", "President", "Law school"};

		// Print the whole database.
		for(String rowlab : rowlabs){
			for(String collab : collabs){
				System.out.println(getCell(db, rowlab, collab));
			}
		}

		// Print all the rows.
		for(String rowlab : rowlabs){
			Map<String,Object> info = getRow(db, rowlab);
			for(Map.Entry<String,Object> kv : info.entrySet()){
				System.out.println(kv.getKey() + " "
							+ kv.getValue().toString());
			}
		}

		// Print all the columns.
		for(String collab : collabs){
			Map<String,Object> info = getColumn(db, collab);
			for(Map.Entry<String,Object> kv : info.entrySet()){
				System.out.println(kv.getKey() + " "
						+ kv.getValue().toString());
			}
		}

		// Replace a row. (Oh no! John Paul Stevens is coming back!)
		TreeMap<String,Object> JPSInfo = new TreeMap<String,Object>();
		JPSInfo.put("Name", "John Paul Stevens");
		JPSInfo.put("President", "Ford");
		JPSInfo.put("Law school", "Northwestern");
		setRow(db, "3", JPSInfo);

		// Print all the rows.
		for(String rowlab : rowlabs){
			Map<String,Object> info = getRow(db, rowlab);
			for(Map.Entry<String,Object> kv : info.entrySet()){
				System.out.println(kv.getKey() + " "
							+ kv.getValue().toString());
			}
		}

		// Print all the columns.
		for(String collab : collabs){
			Map<String,Object> info = getColumn(db, collab);
			for(Map.Entry<String,Object> kv : info.entrySet()){
				System.out.println(kv.getKey() + " "
						+ kv.getValue().toString());
			}
		}

		// Replace a column--rewrite Presidents with full names.
		TreeMap<String,Object> presInfo = new TreeMap<String,Object>();
		presInfo.put("0", "George W. Bush");
		presInfo.put("1", "Barack H. Obama");
		presInfo.put("2", "William J. Clinton");
		presInfo.put("3", "Gerald R. Ford");
		presInfo.put("4", "Ronald W. Reagan");
		presInfo.put("6", "William J. Clinton");
		presInfo.put("8", "George W. Bush");
		presInfo.put("9", "Ronald W. Reagan");
		presInfo.put("10", "George H.W. Bush");
		setColumn(db, "President", presInfo);

		// Print all the rows.
		for(String rowlab : rowlabs){
			Map<String,Object> info = getRow(db, rowlab);
			for(Map.Entry<String,Object> kv : info.entrySet()){
				System.out.println(kv.getKey() + " "
							+ kv.getValue().toString());
			}
		}

		// Print all the columns.
		for(String collab : collabs){
			Map<String,Object> info = getColumn(db, collab);
			for(Map.Entry<String,Object> kv : info.entrySet()){
				System.out.println(kv.getKey() + " "
						+ kv.getValue().toString());
			}
		}
	}

	public static void main(String[] args) {
		clearSubspace(db, table);
		smokeTest();
	}

}
