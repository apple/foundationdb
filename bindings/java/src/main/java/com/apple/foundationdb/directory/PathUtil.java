/*
 * PathUtil.java
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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * The {@code PathUtil} class provides static helper functions useful for working
 * with directory paths.
 */
public class PathUtil {
	/**
	 * Joins two paths into one larger path.
	 *
	 * @param path1 The first path to join
	 * @param path2 The path to append to {@code path1}
	 * @return a new list which contains all the items in {@code path1} followed
	 * by all the items in {@code path2}
	 */
	public static List<String> join(List<String> path1, List<String> path2) {
		List<String> newPath = new LinkedList<String>(path1);
		newPath.addAll(path2);
		return newPath;
	}

	/**
	 * Extends a path by an arbitrary number of elements.
	 *
	 * @param path The path to extend
	 * @param subPaths The items to append to path
	 * @return a new list which contains all the items in {@code path} followed
	 * by all additional items specified in {@code subPaths}
	 */
	public static List<String> extend(List<String> path, String... subPaths) {
		return join(path, Arrays.asList(subPaths));
	}

	/**
	 * Creates a new path from an arbitrary number of elements.
	 *
	 * @param subPaths The items in the path
	 * @return a list which contains all the items specified in {@code subPaths}
	 */
	public static List<String> from(String... subPaths) {
		return new LinkedList<String>(Arrays.asList(subPaths));
	}

	/**
	 * Removes the first item from a path.
	 *
	 * @param path the path whose first item is being popped
	 * @return a new list which contains all the items in {@code path} except
	 * for the first item
	 */
	public static List<String> popFront(List<String> path) {
		if(path.isEmpty())
			throw new IllegalStateException("Path contains no elements.");

		return new LinkedList<String>(path.subList(1, path.size()));
	}

	/**
	 * Removes the last item from a path.
	 *
	 * @param path the path whose last item is being popped
	 * @return a new list which contains all the items in {@code path} except
	 * for the last item
	 */
	public static List<String> popBack(List<String> path) {
		if(path.isEmpty())
			throw new IllegalStateException("Path contains no elements.");

		return new LinkedList<String>(path.subList(0, path.size() - 1));
	}

	private PathUtil() {}
}
