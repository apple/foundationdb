/*
 * package-info.java
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

/**
 * Provides an API for the FoundationDB transactional key/value store. Clients operating
 *  on a {@link com.apple.foundationdb.Database} should, in most cases, use the
 *  {@link com.apple.foundationdb.TransactionContext#run(Function) run(Function)}
 *  or the
 *  {@link com.apple.foundationdb.TransactionContext#runAsync(Function) runAsync(Function)}
 *  constructs. These two functions (and their two derivations) implement a proper
 *  retry loop around the work that needs to get done and, in the case of {@code Database},
 *  assure that {@link com.apple.foundationdb.Transaction#commit()} has returned successfully
 *  before itself returning. If you are not able to use these functions for some reason
 *  please closely read and understand the other
 *  <a href="/foundationdb/data-modeling.html#data-modeling-tuples">developer
 *    documentation on FoundationDB transactions</a>.
 */
package com.apple.foundationdb;
