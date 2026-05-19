/*
 * RequiresDatabase.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

import java.util.Optional;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Rule to make it easy to write integration tests that only work when a running
 * database is detected and connectable using the default cluster file. Use this
 * as a @ClassRule on any integration test that requires a running database.
 *
 * This will attempt to connect to an FDB instance and perform a basic
 * operation. If it can do so quickly, then it will go ahead and run the
 * underlying test statement. If it cannot perform a basic operation against the
 * running DB, then it will throw an error and fail all tests
 *
 * There is a second safety valve--you can also set the env variable
 * `run.integration.tests` to false. If it's set, then all tests will just be
 * skipped outright, without trying to connect. This is useful for when you know you won't
 * be running a server and you don't want to deal with spurious test failures.
 */
public class RequiresDatabase implements ExecutionCondition, BeforeAllCallback {
	private static boolean networkOptionsSet = false;

	public static boolean canRunIntegrationTest() {
		String prop = System.getProperty("run.integration.tests");
		if (prop == null) {
			return true;
		}
		return Boolean.parseBoolean(prop);
	}

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {
		if (canRunIntegrationTest()) {
			return ConditionEvaluationResult.enabled("Database is running");
		} else {
			return ConditionEvaluationResult.disabled("Database is not running");
		}
	}

	private static final int HEALTH_CHECK_TIMEOUT_MS = 5000;
	private static final int HEALTH_CHECK_MAX_ATTEMPTS = 10;
	private static final int HEALTH_CHECK_BACKOFF_MS = 500;

	@Override
	public void beforeAll(ExtensionContext context) throws Exception {
		/*
		 * This is in place to validate that a database is actually running. If it can't connect
		 * within a reasonable time, then the tests automatically fail.
		 *
		 * This is in place mainly to fail-fast in the event of bad configurations; specifically, if the env flag
		 * is set to true (or absent), but a backing server isn't actually running. When that happens, this check avoids
		 * a long hang-time while waiting for the first database connection to finally timeout (which could take a
		 * while, based on empirical observation)
		 *
		 * We retry the health check several times because on heavily loaded CI infrastructure,
		 * the single-node cluster may still be bootstrapping when tests start.
		 *
		 * Note that JUnit will only call this method _after_ calling evaluateExecutionCondition(), so we can safely
		 * assume that if we are here, then canRunIntegrationTest() is returning true and we don't have to bother
		 * checking it.
		 */
		FDB fdb = FDB.selectAPIVersion(ApiVersion.LATEST);
		if (!networkOptionsSet) {
			networkOptionsSet = true;
			Optional<String> externalClientLibrary = context.getConfigurationParameter("external_client_library");
			if (externalClientLibrary.isPresent()) {
				System.err.printf("external_client_library : %s\n", externalClientLibrary.get());
				fdb.options().setExternalClientLibrary(externalClientLibrary.get());
				fdb.options().setDisableLocalClient();
			}
		}

		try (Database db = fdb.open()) {
			for (int attempt = 1; attempt <= HEALTH_CHECK_MAX_ATTEMPTS; attempt++) {
				try {
					db.run(tr -> {
						tr.options().setTimeout(HEALTH_CHECK_TIMEOUT_MS);
						return tr.get("test".getBytes()).join();
					});
					return; // success
				} catch (CompletionException e) {
					if (attempt == HEALTH_CHECK_MAX_ATTEMPTS) {
						Assertions.fail("Test " + context.getDisplayName() +
						                " failed to start: cannot connect to database after " +
						                HEALTH_CHECK_MAX_ATTEMPTS + " attempts: " + e.getMessage());
					}
					Thread.sleep(HEALTH_CHECK_BACKOFF_MS);
				}
			}
		}
	}
}
