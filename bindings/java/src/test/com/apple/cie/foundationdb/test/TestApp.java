/*
 * TestApp.java
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

package com.apple.cie.foundationdb.test;

import com.apple.cie.foundationdb.Cluster;
import com.apple.cie.foundationdb.Database;
import com.apple.cie.foundationdb.FDB;
import com.apple.cie.foundationdb.FDBException;
import com.apple.cie.foundationdb.Transaction;
import com.apple.cie.foundationdb.async.Future;

public class TestApp {

	public static void main(String[] args) throws Exception {
		try {
			Cluster cluster = FDB.selectAPIVersion(510).createCluster("C:\\Users\\Ben\\workspace\\fdb\\fdb.cluster");
			System.out.println("I now have the cluster");
			Database db = cluster.openDatabase();

			Transaction tr = db.createTransaction();
			System.out.println("TR: " + tr);

			byte[] appleValue = tr.get("apple".getBytes()).get();
			System.out.println("Apple: " + (appleValue == null ? null : new String(appleValue)));

			tr.set("apple".getBytes(), "crunchy".getBytes());
			System.out.println("Attmepting to commit apple/crunchy...");
			tr.commit().get(); // FIXME: this is not an ok use of the API
			tr = db.createTransaction();

			long topTime = 0, blockTime = 0, getTime = 0, bottomTime = 0;

//			Future<Void> commit = null;
			for(int i = 0; i < 1000; i++) {
/*				if(commit != null)
					commit.get();*/

		long a = System.currentTimeMillis();
				final int idx = i;
				final byte[] key = ("apple" + idx).getBytes();
				tr = db.createTransaction();
				//System.out.println("TR (" + i + "): " + tr);
				Future<byte[]> future = tr.get(key);
		long b = System.currentTimeMillis();
				future.blockUntilReady();
		long c = System.currentTimeMillis();
				appleValue = future.get();
		long d = System.currentTimeMillis();
				/*boolean present = appleValue != null;
				System.out.println("Value is " +
						(present ? "present" : "missing") );
				if(present)
					System.out.println("Value is <" + new String(appleValue) + ">");*/
				tr.set(key, ("Apple" + i).getBytes());
				final Future<Void> commit = tr.commit();
		long e = System.currentTimeMillis();
				commit.onReady(new Runnable() {
					@Override
					public void run() {
						try {
							commit.get();
						} catch (FDBException e) {
							e.printStackTrace();
						}
					}
				});

		topTime += b - a;
		blockTime += c - b;
		getTime += d - c;
		bottomTime += e - d;
			}

			System.out.println(" Top:    " + topTime);
			System.out.println(" Block:  " + blockTime);
			System.out.println(" Get:    " + getTime);
			System.out.println(" Bottom: " + bottomTime);

			tr.dispose();
			db.dispose();
			cluster.dispose();
//			commit.get();
		} catch(Throwable t) {
			t.printStackTrace();
		}
	}
}
