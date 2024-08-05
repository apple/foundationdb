/*
 * MicroBlob.java
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

public class MicroBlob {

	private static final FDB fdb;
	private static final Database db;
	private static final Subspace blob;
	private static final int CHUNK_SIZE = 5;

	static{
		fdb = FDB.selectAPIVersion(300);
		db = fdb.open();
		blob = new Subspace(Tuple.from("B"));
	}

	// Clears subspace.
	public static void clearSubspace(TransactionContext tcx, final Subspace s){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				tr.clear(Range.startsWith(s.getKey()));
				return null;
			}
		});
	}

	// TODO This method is in the recipe book.
	// Writes to the blobs.
	public static void writeBlob(TransactionContext tcx, final String data){
		if(data.length() == 0) return;

		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr) {
				int numChunks = (data.length() + CHUNK_SIZE - 1)/CHUNK_SIZE;
				int chunkSize = (data.length() + numChunks)/numChunks;

				for(int i = 0; i*chunkSize < data.length(); i++){
					int start = i*chunkSize;
					int end  = ((i+1)*chunkSize <= data.length()) ? ((i+1)*chunkSize) : (data.length());
					tr.set(blob.subspace(Tuple.from(start)).pack(),
							Tuple.from(data.substring(start, end)).pack());
				}
				return null;
			}
		});
	}

	// TODO This method is in the recipe book.
	// Reads from the blobs.
	public static String readBlob(TransactionContext tcx){
		return tcx.run(new Function<Transaction,String>() {
			public String apply(Transaction tr){
				StringBuilder value = new StringBuilder();

				for(KeyValue kv : tr.getRange(blob.range())){
					value.append(Tuple.fromBytes(kv.getValue()).getString(0));
				}

				return value.toString();
			}
		});
	}

	public static void main(String[] args){
		clearSubspace(db, blob);
		writeBlob(db, "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
		System.out.println(readBlob(db));
	}
}
