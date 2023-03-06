/*
 * GetClientStatusIntegrationTest.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2023 Apple Inc. and the FoundationDB project authors
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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Integration tests around Range Queries. This requires a running FDB instance to work properly;
 * all tests will be skipped if it can't connect to a running instance relatively quickly.
 */
class GetClientStatusIntegrationTest {
	private static final FDB fdb = FDB.selectAPIVersion(ApiVersion.LATEST);

	@Test
	public void clientStatusIsHealthy() throws Exception {
		try (Database db = fdb.open()) {
			// Run a simple transaction to make sure the database is fully initialized
			db.run(tr -> {
				return tr.getReadVersion().join();
			});

			// Here we just check if a meaningful client report status is returned
			// Different report attributes and error cases are covered by C API tests
			String statusStr = new String(db.getClientStatus().join());
			Assertions.assertTrue(statusStr.contains("\"Healthy\":true"),
				String.format("Healthy:true not found in client status: %s", statusStr));
		}
	}
}
