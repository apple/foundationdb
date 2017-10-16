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
import com.apple.cie.foundationdb.async.Function;
import com.apple.cie.foundationdb.async.Future;
import com.apple.cie.foundationdb.async.PartialFunction;
import com.apple.cie.foundationdb.async.PartialFuture;
import com.apple.cie.foundationdb.async.ReadyFuture;

public class AsListTest {
	/**
	 * When the database contains keys a, b, c, d, e -- this should return 5 items,
	 *  a bug made the the addition of the clear into the result returning 0 items.
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
		String[] keyAddresses = LocalityUtil.getAddressesForKey(tr, "a".getBytes()).get();
		for(String s : keyAddresses) {
			System.out.println(" @ " + s);
		}

		@SuppressWarnings("unused")
		Future<Integer> i = AsyncUtil.applySafely(new Function<Exception, Future<Integer>>() {
			@Override
			public Future<Integer> apply(Exception o) {
				return new ReadyFuture<Integer>(3);
			}
		}, new RuntimeException());

		Future<Integer> f = null;
		PartialFuture<Integer> pf = null;

		@SuppressWarnings({ "unused", "null" })
		Future<String> g = f.flatMap(new Function<Integer, Future<String>>() {
			@Override
			public Future<String> apply(Integer o) {
				return new ReadyFuture<String>( o.toString() );
			}
		});

		@SuppressWarnings("unused")
		PartialFuture<String> h = f.flatMap(new PartialFunction<Integer, Future<String>>() {
			@Override
			public Future<String> apply(Integer o) throws Exception {
				if (o == null) throw new Exception("AHH");
				return new ReadyFuture<String>( o.toString() );
			}
		});

		@SuppressWarnings("unused")
		PartialFuture<String> h2 = f.flatMap(new PartialFunction<Integer, PartialFuture<String>>() {
			@Override
			public PartialFuture<String> apply(Integer o) throws Exception {
				if (o == null) throw new Exception("AHH");
				return new ReadyFuture<String>( o.toString() );
			}
		});

		@SuppressWarnings({ "unused", "null" })
		Future<String> g2 = pf.flatMap(new Function<Integer, Future<String>>() {
			@Override
			public Future<String> apply(Integer o) {
				return new ReadyFuture<String>( o.toString() );
			}
		}).rescue(new Function<Exception, Future<String>>() {
			@Override
			public Future<String> apply(Exception o) {
				// TODO Auto-generated method stub
				return null;
			}
		});
	}
}
