/*
 * GetRangeWithPredicateTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
package com.apple.foundationdb;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(RequiresDatabase.class)
class GetRangeWithPredicateTest {
	private static final FDB fdb = FDB.selectAPIVersion(710);

	@BeforeEach
	@AfterEach
	void clearDatabase() throws Exception {
		/*
		 * Empty the database before and after each run, just in case
		 */
		try (Database db = fdb.open()) {
			db.run(tr -> {
				tr.clear(new byte[] {}, new byte[] { (byte)255 });
				return null;
			});
		}
	}

	@Test
	void testSomething() {
		try (Database db = fdb.open()) {
			db.run(tr -> {
				tr.set("foo00".getBytes(), "baz".getBytes());
				tr.set("foo01".getBytes(), "bar".getBytes());
				tr.set("foo02".getBytes(), "baz".getBytes());
				return null;
			});
			db.run(tr -> {
				tr.options().setReadYourWritesDisable();
				List<KeyValue> result =
				    tr.getRangeWithPredicate("foo00".getBytes(), "foo99".getBytes(), "std/findInVal".getBytes(),
				                             new byte[][] { "bar".getBytes() })
				        .join();
				assertEquals(1, result.size());
				return null;
			});
		}
	}
}
