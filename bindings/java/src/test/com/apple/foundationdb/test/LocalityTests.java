/*
 * LocalityTests.java
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

package com.apple.foundationdb.test;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.LocalityUtil;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.CloseableAsyncIterator;
import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.tuple.ByteArrayUtil;

public class LocalityTests {

	public static void main(String[] args) {
		FDB fdb = FDB.selectAPIVersion(720);
		try(Database database = fdb.open(args[0])) {
			try(Transaction tr = database.createTransaction()) {
				String[] keyAddresses = LocalityUtil.getAddressesForKey(tr, "a".getBytes()).join();
				for(String s : keyAddresses) {
					System.out.println(" @ " + s);
				}
			}

			long start = System.currentTimeMillis();

			try(CloseableAsyncIterator<byte[]> keys = LocalityUtil.getBoundaryKeys(database, new byte[0], new byte[]{(byte) 255})) {
				CompletableFuture<List<byte[]>> collection = AsyncUtil.collectRemaining(keys);
				List<byte[]> list = collection.join();
				System.out.println("Took " + (System.currentTimeMillis() - start) + "ms to get " +
						list.size() + " items");

				int i = 0;
				for(byte[] key : collection.join()) {
					System.out.println(i++ + ": " + ByteArrayUtil.printable(key));
				}
			}
		}
	}

	private LocalityTests() {}
}
