/*
 * FDBLibraryRule.java
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
package com.apple.foundationdb;

import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * A Rule that pre-loads the native library on demand, for testing purposes.
 * This is mainly for convenience to avoid needing to worry about which version
 * number is correct and whatnot. It will fail to work if the native libraries
 * are not available for any reason.
 */
public class FDBLibraryRule implements BeforeAllCallback {
	public static final int CURRENT_API_VERSION = 720;

	private final int apiVersion;

	// because FDB is a singleton (currently), this isn't a super-useful cache,
	// but it does make life slightly easier, so we'll keep it around
	private FDB instance;

	public FDBLibraryRule(int apiVersion) { this.apiVersion = apiVersion; }

	public static FDBLibraryRule current() { return new FDBLibraryRule(CURRENT_API_VERSION); }

	public static FDBLibraryRule v63() { return new FDBLibraryRule(630); }

	public FDB get() { return instance; }

	@Override
	public void beforeAll(ExtensionContext arg0) throws Exception {
		instance = FDB.selectAPIVersion(apiVersion);
	}
}
