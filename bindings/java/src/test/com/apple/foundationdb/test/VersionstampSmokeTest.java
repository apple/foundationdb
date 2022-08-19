/*
 * VersionstampSmokeTest.java
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

import java.util.concurrent.CompletableFuture;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;
import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;

public class VersionstampSmokeTest {
	public static void main(String[] args) {
		FDB fdb = FDB.selectAPIVersion(720);
		try(Database db = fdb.open()) {
			db.run(tr -> {
				tr.clear(Tuple.from("prefix").range());
				return null;
			});

			CompletableFuture<byte[]> trVersionFuture = db.run((Transaction tr) -> {
				// The incomplete Versionstamp will have tr's version information when committed.
				Tuple t = Tuple.from("prefix", Versionstamp.incomplete());
				tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, t.packWithVersionstamp(), new byte[0]);
				return tr.getVersionstamp();
			});

			byte[] trVersion = trVersionFuture.join();

			Versionstamp v = db.run((Transaction tr) -> {
				Subspace subspace = new Subspace(Tuple.from("prefix"));
				byte[] serialized = tr.getRange(subspace.range(), 1).iterator().next().getKey();
				Tuple t = subspace.unpack(serialized);
				return t.getVersionstamp(0);
			});

			System.out.println(v);
			System.out.println(Versionstamp.complete(trVersion));
			assert v.equals(Versionstamp.complete(trVersion));
		}
	}

	private VersionstampSmokeTest() {}
}
