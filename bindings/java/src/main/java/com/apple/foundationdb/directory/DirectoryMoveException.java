/*
 * DirectoryMoveException.java
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

package com.apple.foundationdb.directory;

import java.util.List;

/**
 * An {@link Exception} that is thrown when an invalid directory move
 * is attempted.
 */
@SuppressWarnings("serial")
public class DirectoryMoveException extends RuntimeException {
	/**
	 * The path of the directory being moved.
	 */
	public final List<String> sourcePath;

	/**
	 * The path that the directory was being moved to.
	 */
	public final List<String> destPath;

	DirectoryMoveException(String message, List<String> sourcePath, List<String> destPath) {
		super(message + ": sourcePath=" + DirectoryUtil.pathStr(sourcePath) + ", destPath=" + DirectoryUtil.pathStr(destPath));
		this.sourcePath = sourcePath;
		this.destPath = destPath;
	}
}

