/*
 * DirectoryTest.java
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionException;

import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for directory logic in FDB. This test requires a running
 * FDB instance to work properly; if one cannot be detected, all tests will be
 * skipped.
 */
@ExtendWith(RequiresDatabase.class)
class DirectoryTest {
	private static final FDB fdb = FDB.selectAPIVersion(720);

	@Test
	void testCanCreateDirectory() throws Exception {
		/*
		 * Simple test to make sure we can create a directory
		 */
		final DirectoryLayer dir = new DirectoryLayer();
		try (Database db = fdb.open()) {

			db.run(tr -> {
				List<String> path = new ArrayList<>();
				path.add("toCreate");
				try {
					DirectorySubspace foo = dir.create(tr, path).join();
					Assertions.assertIterableEquals(path, foo.getPath(), "Incorrect path");
					// make sure it exists
					Assertions.assertTrue(foo.exists(tr).join(), "does not exist even though it's been created!");

				} finally {
					// remove the directory
					dir.remove(tr, path).join();
				}
				return null;
			});
		}
	}

	@Test
	void testCanCreateSubDirectory() throws Exception {
		/*
		 * Test that we can create a subdirectory safely
		 */
		final DirectoryLayer dir = new DirectoryLayer();
		try (Database db = fdb.open()) {

			db.run(tr -> {
				List<String> path = new ArrayList<>();
				path.add("foo");
				try {
					DirectorySubspace foo = dir.create(tr, path).join();
					Assertions.assertIterableEquals(path, foo.getPath(), "Incorrect path");
					// make sure it exists
					Assertions.assertTrue(foo.exists(tr).join(), "does not exist even though it's been created!");

					path.add("bar");
					DirectorySubspace bar = dir.create(tr, path).join();
					Assertions.assertIterableEquals(path, bar.getPath(), "incorrect path");
					Assertions.assertTrue(bar.exists(tr).join(), "does not exist even though it's been created!");
				} finally {
					// remove the directory
					dir.remove(tr, path).join();
				}
				return null;
			});
		}
	}

	@Test
	void testCanMoveSubDirectory() throws Exception {
		/*
		 * Make sure that we can move a subdirectory correctly
		 */
		final DirectoryLayer dir = new DirectoryLayer();
		try (Database db = fdb.open()) {

			db.run(tr -> {
				List<String> path = new ArrayList<>();
				path.add("src");
				try {
					DirectorySubspace foo = dir.create(tr, path).join();
					Assertions.assertIterableEquals(path, foo.getPath(), "Incorrect path");
					// make sure it exists
					Assertions.assertTrue(foo.exists(tr).join(), "does not exist even though it's been created!");

					path.add("bar");
					DirectorySubspace bar = dir.create(tr, path).join();
					Assertions.assertIterableEquals(path, bar.getPath(), "incorrect path");
					Assertions.assertTrue(bar.exists(tr).join(), "does not exist even though it's been created!");

					DirectorySubspace boo = dir.create(tr, Arrays.asList("dest")).join();
					Assertions.assertIterableEquals(Arrays.asList("dest"), boo.getPath(), "incorrect path");
					Assertions.assertTrue(boo.exists(tr).join(), "does not exist even though it's been created!");

					// move the subdirectory and see if it works
					DirectorySubspace newBar = bar.moveTo(tr, Arrays.asList("dest", "bar")).join();
					Assertions.assertIterableEquals(Arrays.asList("dest", "bar"), newBar.getPath(), "incorrect path");
					Assertions.assertTrue(newBar.exists(tr).join(), "does not exist even though it's been created!");
					Assertions.assertFalse(bar.exists(tr).join(), "Still exists in old location!");

				} finally {
					// remove the directory
					dir.remove(tr, Arrays.asList("src")).join();
					try {
						dir.remove(tr, Arrays.asList("dest")).join();
					} catch (CompletionException ce) {
						Throwable t = ce.getCause();
						if (!(t instanceof NoSuchDirectoryException)) {
							throw ce;
						}
					}
				}
				return null;
			});
		}
	}

	@Test
	void testCannotCreateDirectoryTwice() throws Exception {
		/*
		 * Shouldn't be able to create the directory twice--make sure it throws the
		 * proper error if we try.
		 */
		final DirectoryLayer dir = new DirectoryLayer();

		try (Database db = fdb.open()) {

			db.run(tr -> {
				List<String> path = new ArrayList<>();
				path.add("foo");
				try {
					DirectorySubspace foo = dir.createOrOpen(tr, path).join();
					Assertions.assertEquals(path, foo.getPath(), "Incorrect path");
					// make sure it exists
					Assertions.assertTrue(foo.exists(tr).join(), "does not exist even though it's been created!");

					// now try to create it again
					try {
						DirectorySubspace foo2 = dir.create(tr, path).join();
						Assertions.fail("Was able to create a directory twice");
					} catch (DirectoryAlreadyExistsException expected) {
					} catch (CompletionException ce) {
						Throwable t = ce.getCause();
						if (!(t instanceof DirectoryAlreadyExistsException)) {
							throw ce;
						}
					}

				} finally {
					// remove the directory
					dir.remove(tr, path).join();
				}
				return null;
			});
		}
	}

	@Test
	void testCannotRemoveNonexistingDirectory() throws Exception {
		/*
		 * can't remove a directory that's not there--should throw a
		 * NoSuchDirectoryException
		 */
		final DirectoryLayer dir = new DirectoryLayer();

		try (Database db = fdb.open()) {
			db.run(tr -> {
				try {
					dir.remove(tr, Arrays.asList("doesnotexist")).join();
				} catch (CompletionException ce) {
					Throwable t = ce.getCause();
					if (!(t instanceof NoSuchDirectoryException)) {
						throw ce;
					}
				}
				return null;
			});
		}
	}
}
