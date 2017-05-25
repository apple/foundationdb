/*
 * AsListTest.java
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

import com.apple.cie.foundationdb.Database;
import com.apple.cie.foundationdb.FDB;
import com.apple.cie.foundationdb.LocalityUtil;
import com.apple.cie.foundationdb.Transaction;
import com.apple.cie.foundationdb.async.AsyncUtil;

import java.util.function.Function;
import java.util.concurrent.CompletableFuture;

public class AsListTest {
	/**
	 * When the database contains keys a, b, c, d, e -- this should return 5 items,
	 * a bug made the addition of the clear into the result returning 0 items.
	 */
	public static void main(String[] args) {
		FDB fdb = FDB.selectAPIVersion(500);
		Database database = fdb.open("T:\\circus\\tags\\RebarCluster-bbc\\cluster_id.txt");
		database.options().setLocationCacheSize(42);
		Transaction tr = database.createTransaction();
		//tr.clear("g".getBytes());
		/*tr.clear("bbb".getBytes());
		AsyncIterable<KeyValue> query = tr.getRange(
				KeySelector.firstGreaterOrEqual("a".getBytes()),
				KeySelector.firstGreaterOrEqual("e".getBytes()),
				Integer.MAX_VALUE);
		//List<KeyValue> list = query.asList().get();
		//System.out.println("List size: " + list.size());
*/
		String[] keyAddresses = LocalityUtil.getAddressesForKey(tr, "a".getBytes()).join();
		for(String s : keyAddresses) {
			System.out.println(" @ " + s);
		}

		@SuppressWarnings("unused")
		CompletableFuture<Integer> i = AsyncUtil.applySafely(new Function<Exception, CompletableFuture<Integer>>() {
			@Override
			public CompletableFuture<Integer> apply(Exception o) {
				return CompletableFuture.completedFuture(3);
			}
		}, new RuntimeException());

		CompletableFuture<Integer> f = null;

		@SuppressWarnings({ "unused", "null" })
		CompletableFuture<String> g = f.thenComposeAsync(new Function<Integer, CompletableFuture<String>>() {
			@Override
			public CompletableFuture<String> apply(Integer o) {
				return CompletableFuture.completedFuture(o.toString());
			}
		});

		@SuppressWarnings({ "unused", "null" })
		CompletableFuture<String> g2 = f.thenComposeAsync(new Function<Integer, CompletableFuture<String>>() {
			@Override
			public CompletableFuture<String> apply(Integer o) {
				return CompletableFuture.completedFuture(o.toString());
			}
		}).exceptionally(new Function<Throwable, String>() {
			@Override
			public String apply(Throwable o) {
				// TODO Auto-generated method stub
				return null;
			}
		});
	}
}
