/*
 * DirectoryOperation.java
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

enum DirectoryOperation {
	DIRECTORY_CREATE_SUBSPACE(true),
	DIRECTORY_CREATE_LAYER(true),
	DIRECTORY_CHANGE,
	DIRECTORY_SET_ERROR_INDEX,
	DIRECTORY_CREATE_OR_OPEN(true),
	DIRECTORY_CREATE(true),
	DIRECTORY_OPEN(true),
	DIRECTORY_MOVE(true),
	DIRECTORY_MOVE_TO(true),
	DIRECTORY_REMOVE,
	DIRECTORY_REMOVE_IF_EXISTS,
	DIRECTORY_LIST,
	DIRECTORY_EXISTS,
	DIRECTORY_CHECK_LAYER,
	DIRECTORY_PACK_KEY,
	DIRECTORY_UNPACK_KEY,
	DIRECTORY_RANGE,
	DIRECTORY_CONTAINS,
	DIRECTORY_OPEN_SUBSPACE(true),
	DIRECTORY_LOG_SUBSPACE,
	DIRECTORY_LOG_DIRECTORY,
	DIRECTORY_STRIP_PREFIX;

	boolean createsDirectory;

	DirectoryOperation() {
		this(false);
	}

	DirectoryOperation(boolean createsDirectory) {
		this.createsDirectory = createsDirectory;
	}
}

