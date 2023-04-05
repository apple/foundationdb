/*
 * AbstractTester.java
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

import java.nio.charset.Charset;
import java.util.Random;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDB;

public abstract class AbstractTester {
	public static final int API_VERSION = 720;
	protected static final int NUM_RUNS = 25;
	protected static final Charset ASCII = Charset.forName("ASCII");

	protected TesterArgs args;
	protected Random random;
	protected TestResult result;
	protected FDB fdb;

	public AbstractTester() {
		args = null;
		random = new Random();
		result = new TestResult(random);
	}

	public void runTest() {
		try(Database db = fdb.open()) {
			try {
				testPerformance(db);
			}
			catch (Exception e) {
				result.addError(wrapAndPrintError(e, "Failed to complete all tests"));
			}
		}
		catch (Exception e) {
			result.addError(wrapAndPrintError(e, "fdb.open failed"));
			return;
		}
	}

	public abstract void testPerformance(Database db);

	public String multiVersionDescription() {
		if (args == null) return "";

		if (!args.useMultiversionApi()) {
			return "multi-version API disabled";
		} else if (args.useExternalClient()) {
			if (args.putCallbacksOnExternalThread()) {
				return "external client on external thread";
			} else {
				return "external client on main thread";
			}
		} else {
			return "local client";
		}
	}

	public void run(String[] argStrings) {
		args = TesterArgs.parseArgs(argStrings);
		if (args == null) return;

		fdb = FDB.selectAPIVersion(API_VERSION);

		// Validate argument combinations and set options.
		if (!args.useMultiversionApi()) {
			if (args.putCallbacksOnExternalThread() || args.useExternalClient()) {
				throw new IllegalArgumentException("Invalid multi-version API argument combination");
			}
			fdb.options().setDisableMultiVersionClientApi();
		}
		if (args.putCallbacksOnExternalThread()) {
			if (!args.useExternalClient()) {
				throw new IllegalArgumentException("Cannot enable callbacks on external thread without using external client");
			}
			fdb.options().setCallbacksOnExternalThreads();
		}
		if (args.useExternalClient()) {
			fdb.options().setDisableLocalClient();
		}

		try {
			runTest();
		} catch (Exception e) {
			result.addError(e);
		}

		result.save(args.getOutputDirectory());
	}

	public RuntimeException wrapAndPrintError(Throwable t, String message) {
		String errorMessage = message + ": " + t.getClass() + ": " + t.getMessage() + "\n";
		t.printStackTrace();
		return new RuntimeException(errorMessage, t);
	}
}
