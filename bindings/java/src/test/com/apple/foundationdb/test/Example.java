/*
 * Example.java
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

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.tuple.Tuple;

public class Example {
	public static void main(String[] args) {
		FDB fdb = FDB.selectAPIVersion(720);

		try(Database db = fdb.open()) {
			// Run an operation on the database
			db.run(tr -> {
				tr.set(Tuple.from("hello").pack(), Tuple.from("world").pack());
				return null;
			});

			// Get the value of 'hello' from the database
			String hello = db.run(tr -> {
				byte[] result = tr.get(Tuple.from("hello").pack()).join();
				return Tuple.fromBytes(result).getString(0);
			});
			System.out.println("Hello " + hello);
		}
	}

	private Example() {}
}
