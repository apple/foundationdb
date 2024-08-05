/*
 * MismatchedLayerException.java
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

package com.apple.foundationdb.directory;

import java.util.List;

import com.apple.foundationdb.tuple.ByteArrayUtil;

/**
 * A {@link DirectoryException} that is thrown when a directory is opened with an incompatible layer.
 */
@SuppressWarnings("serial")
public class MismatchedLayerException extends DirectoryException {
	/**
	 * The layer byte string that the directory was created with.
	 */
	public final byte[] stored;

	/**
	 * The layer byte string that the directory was opened with.
	 */
	public final byte[] opened;

	MismatchedLayerException(List<String> path, byte[] stored, byte[] opened) {
		super("Mismatched layer: stored=" + ByteArrayUtil.printable(stored) + ", opened=" + ByteArrayUtil.printable(opened), path);
		this.stored = stored;
		this.opened = opened;
	}
}

