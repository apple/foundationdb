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
 * Provides a set of utilities for serializing and deserializing typed data
 *  for use in FoundationDB. When packed together into a {@link com.apple.foundationdb.tuple.Tuple}
 *  this data is suitable for use as an index or organizational structure within FoundationDB
 *  keyspace. See <a href="/foundationdb/data-modeling.html#data-modeling-tuples">general Tuple
 *  documentation</a> for information about how Tuples sort and can be used to efficiently
 *  model data.
 */
package com.apple.foundationdb.tuple;
