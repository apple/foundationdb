/*
 * package-info.java
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

/**
 * Provides a convenient way to define namespaces for different categories
 *  of data. The namespace is specified
 *  by a prefix tuple which is prepended to all tuples packed by the subspace.
 *  When unpacking a key with the subspace, the prefix tuple will be removed
 *  from the result. As a best practice, API clients should use at least one
 *  subspace for application data.<br>
 *  <br>
 *  See <a href="/foundationdb/developer-guide.html#developer-guide-sub-keyspaces">general
 *   subspace documentation</a> for information about how subspaces work and
 *   interact with other parts of the built-in keyspace management features.
 */
package com.apple.foundationdb.subspace;
