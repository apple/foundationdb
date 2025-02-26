/*
 * TestApiVersion.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

/**
 * Declares the last tested API version of standalone Java tests & benchmarks
 * that are not running as a part of CI. The tests should be retested
 * manually after updating the API version
 */
public class TestApiVersion {
    /**
     * The current API version to be used by the tests
     */
	public static final int CURRENT = 800;
}
