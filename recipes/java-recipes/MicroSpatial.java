/*
 * MicroSpatial.java
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

public class MicroSpatial {

	private static final FDB fdb;
	private static final Database db;
	private static final Subspace labelZ;
	private static final Subspace zLabel;

	static {
		fdb = FDB.selectAPIVersion(300);
		db = fdb.open();
		labelZ = new Subspace(Tuple.from("L"));
		zLabel = new Subspace(Tuple.from("Z"));
	}

	// TODO These three methods, xyToZ, zToXy, and setLocation, are allin the recipe book.
	public long xyToZ(long[] p){
		long x,y,z;
		x = p[0]; y = p[1];
		// Encode (x,y) as a binary key z.
		return z;
	}

	public long[] zToXy(long z){
		long[] p = new long[2];
		long x, y;
		// Decode z to a pair of coordinates (x,y).
		p[0] = x; p[1] = y;
		return p;
	}

	public void setLocation(TransactionContext tcx, final String label, final long[] pos){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				long z = xyToZ(pos);
				long previous;
				// Read labelZ.subspace(Tuple.from(label)) to find previous z.
				if(/* there is a previous z */){
					tr.clear(labelZ.pack(Tuple.from(label,previous)));
					tr.clear(zLabel.pack(Tuple.from(previous,label)));
				}
				tr.set(labelZ.pack(Tuple.from(label,z)),Tuple.from().pack());
				tr.set(zLabel.pack(Tuple.from(z,label)),Tuple.from().pack());
				return null;
			}
		});
	}
}
