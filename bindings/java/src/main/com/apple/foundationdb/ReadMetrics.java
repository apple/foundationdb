/*
 * ReadMetrics.java
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

/**
 * Defines a set of metrics that can be accessed on a read result.
 */
public interface ReadMetrics {
	/**
	 * Returns a value between 0 and 1 indicating how busy the storage server that served the read request is. A value
	 * of 1 means the storage server is maximally busy, while a value of 0 means that it is idle.
	 *
	 * @return the busyness of the storage server that answered the read request.
	 */
	public float getServerBusyness();

	/**
	 * Returns a value between 0 and 1 indicating what portion of a storage server's read workload is reading keys from
	 * the same range as this read request. The tracked ranges are chosen by the cluster. A value of 1 means that all of
	 * a storage server's read workload is to the same range, while a value of 0 means that none of it is.
	 *
	 * @return the busyness of the range on the storage server that answered the read request.
	 */
	public float getRangeBusyness();
}