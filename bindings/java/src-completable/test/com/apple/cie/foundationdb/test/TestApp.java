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

import java.util.concurrent.CompletableFuture;

import com.apple.cie.foundationdb.Cluster;
import com.apple.cie.foundationdb.Database;
import com.apple.cie.foundationdb.FDB;
import com.apple.cie.foundationdb.Transaction;

public class TestApp {

	public static void main(String[] args) throws Exception {
		try {
			Cluster cluster = FDB.selectAPIVersion(500).createCluster();
			System.out.println("I now have the cluster");
			Database db = cluster.openDatabase();

			Transaction tr = db.createTransaction();
			System.out.println("TR: " + tr);

			byte[] appleValue = tr.get("apple".getBytes()).get();
			System.out.println("Apple: " + (appleValue == null ? null : new String(appleValue)));

			tr.set("apple".getBytes(), "crunchy".getBytes());
			System.out.println("Attempting to commit apple/crunchy...");
			tr.commit().get(); // FIXME: this is not an ok use of the API
            tr = tr.reset();

			long topTime = 0, getTime = 0, bottomTime = 0;

			for(int i = 0; i < 1000; i++) {
                long a = System.currentTimeMillis();

                final byte[] key = ("apple" + i).getBytes();
                tr = db.createTransaction();
                CompletableFuture<byte[]> future = tr.get(key);

                long b = System.currentTimeMillis();

                future.get();

                long c = System.currentTimeMillis();

                tr.set(key, ("Apple" + i).getBytes());
                final CompletableFuture<Void> commit = tr.commit();

                long d = System.currentTimeMillis();

                commit.whenCompleteAsync((v, error) -> {
                    if(error != null) {
                        error.printStackTrace();
                    }
				});

                topTime += b - a;
                getTime += c - b;
                bottomTime += d - c;
			}

			System.out.println(" Top:    " + topTime);
			System.out.println(" Get:    " + getTime);
			System.out.println(" Bottom: " + bottomTime);

			tr.dispose();
			db.dispose();
			cluster.dispose();
		} catch(Throwable t) {
			t.printStackTrace();
		}
	}
}
