/*
 * MicroSpatialTest.java
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

import java.util.Arrays;

import com.foundationdb.*;
import com.foundationdb.async.Function;
import com.foundationdb.subspace.Subspace;
import com.foundationdb.tuple.Tuple;

public class MicroSpatialTest {

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

	public static long xyToZ(long[] p){
		long z = 0l;
		long zb = 0;
		long[] m = Arrays.copyOf(p,2);

		while(m[0] != 0l || m[1] != 0l){
			for(int i = 0; i < 2; i++){
				z += (m[i] & 1) << zb;
				zb += 1;
				m[i] >>= 1;
			}
		}

		return z;
	}

	public static long[] zToXy(long z){
		long[] p = new long[2];
		long l = 0l;

		p[0] = 0l; p[1] = 0l;
		while(z != 0){
			for(int i = 0; i < 2; i++){
				p[i] += (z & 1) << l;
				z >>= 1;
			}
			l += 1;
		}

		return p;
	}

	public static void setLocation(TransactionContext tcx, final String label, final long[] pos){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				long z = xyToZ(pos);
				boolean isPrevious = false;
				long previous = 0l;

				for(KeyValue kv : tr.getRange(labelZ.subspace(Tuple.from(label)).range(),1)){
					isPrevious = true;
					previous = Tuple.fromBytes(kv.getKey()).getLong(2);
				}

				if(isPrevious){
					tr.clear(labelZ.pack(Tuple.from(label,previous)));
					tr.clear(zLabel.pack(Tuple.from(previous,label)));
				}
				tr.set(labelZ.pack(Tuple.from(label,z)),Tuple.from().pack());
				tr.set(zLabel.pack(Tuple.from(z,label)),Tuple.from().pack());
				return null;
			}
		});
	}

	public static long getLocation(TransactionContext tcx, final String label){
		return tcx.run(new Function<Transaction,Long>() {
			public Long apply(Transaction tr){
				for(KeyValue kv : tr.getRange(labelZ.subspace(Tuple.from(label)).range(),1)){
					return Tuple.fromBytes(kv.getKey()).getLong(2);
				}
				return -1l;
			}
		});
	}

	private static String tupleRep(Tuple t){
		StringBuilder sb = new StringBuilder();
		sb.append('(');
		for(Object o : t){
			sb.append(o.toString());
			sb.append(',');
		}
		sb.append(')');
		return sb.toString();
	}

	public static void printSubspace(TransactionContext tcx, final Subspace s){
		tcx.run(new Function<Transaction,Void>() {
			public Void apply(Transaction tr){
				for(KeyValue kv : tr.getRange(s.range())){
					System.out.println(tupleRep(Tuple.fromBytes(kv.getKey())));
				}
				return null;
			}
		});
	}

	public static void main(String[] args){
		db.run(new Function<Transaction,Void>(){
			public Void apply(Transaction tr){
				tr.clear(labelZ.range());
				tr.clear(zLabel.range());
				return null;
			}
		});

		String[] labels = {"a","b","c","d","e"};
		long[][] positions = {{3l,2l},{1l,4l},{5l,3l},{2l,3l},{0l,0l}};

		System.out.println("Point d is at " + getLocation(db, "d"));

//		printSubspace(db, labelZ);
//		printSubspace(db, zLabel);

		for(int i = 0; i < labels.length; i++){
			setLocation(db, labels[i], positions[i]);
		}

//		printSubspace(db, labelZ);
//		printSubspace(db, zLabel);

		System.out.println("Point d is at " + getLocation(db,"d"));

		setLocation(db, "d", positions[0]);
		System.out.println("Point d is at " + getLocation(db,"d"));

//		printSubspace(db,labelZ);
//		printSubspace(db, zLabel);
	}
}
