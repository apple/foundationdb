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

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletionException;

import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.directory.NoSuchDirectoryException;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

/**
 * Integration tests for directory logic in FDB. This test requires a running FDB instance to work properly; 
 * if one cannot be detected, all tests will be skipped.
 */
public class DirectoryTest {
    @ClassRule
    public static final TestRule dbRule = RequiresDatabase.require();
    private static final FDB fdb = FDB.selectAPIVersion(700);

    @Test
    public void testCanCreateDirectory() throws Exception {
        /*
         * Simple test to make sure we can create a directory
         */
        final DirectoryLayer dir = new DirectoryLayer();
        try (Database db = fdb.open()) {

            db.run(tr -> {
                List<String> path = new ArrayList<>();
                path.add("toCreate");
                Path correctPath = Paths.get("toCreate");
                try {
                    DirectorySubspace foo = dir.create(tr, path).join();
                    Assert.assertEquals("Incorrect path", correctPath, toPathObject(foo.getPath()));
                    // make sure it exists
                    Assert.assertTrue("does not exist even though it's been created!", foo.exists(tr).join());

                } finally {
                    // remove the directory
                    dir.remove(tr, path).join();
                }
                return null;
            });
        }
    }

    @Test
    public void testCanCreateSubDirectory() throws Exception {
        /*
         * Test that we can create a subdirectory safely
         */
        final DirectoryLayer dir = new DirectoryLayer();
        try (Database db = fdb.open()) {

            db.run(tr -> {
                List<String> path = new ArrayList<>();
                path.add("foo");
                Path correctPath = Paths.get("foo");
                try {
                    DirectorySubspace foo = dir.create(tr, path).join();
                    Assert.assertEquals("Incorrect path", correctPath, toPathObject(foo.getPath()));
                    // make sure it exists
                    Assert.assertTrue("does not exist even though it's been created!", foo.exists(tr).join());

                    path.add("bar");
                    correctPath = correctPath.resolve("bar");
                    DirectorySubspace bar = dir.create(tr,path).join();
                    Assert.assertEquals("incorrect path",correctPath, toPathObject(bar.getPath()));
                    Assert.assertTrue("does not exist even though it's been created!", bar.exists(tr).join());
                } finally {
                    // remove the directory
                    dir.remove(tr, path).join();
                }
                return null;
            });
        }
    }

    @Test
    public void testCanMoveSubDirectory() throws Exception {
        /*
         * Make sure that we can move a subdirectory correctly
         */
        final DirectoryLayer dir = new DirectoryLayer();
        try (Database db = fdb.open()) {

            db.run(tr -> {
                List<String> path = new ArrayList<>();
                path.add("src");
                Path correctPath = Paths.get("src");
                try {
                    DirectorySubspace foo = dir.create(tr, path).join();
                    Assert.assertEquals("Incorrect path", correctPath, toPathObject(foo.getPath()));
                    // make sure it exists
                    Assert.assertTrue("does not exist even though it's been created!", foo.exists(tr).join());

                    path.add("bar");
                    correctPath = correctPath.resolve("bar");
                    DirectorySubspace bar = dir.create(tr,path).join();
                    Assert.assertEquals("incorrect path",correctPath, toPathObject(bar.getPath()));
                    Assert.assertTrue("does not exist even though it's been created!", bar.exists(tr).join());

                    DirectorySubspace boo = dir.create(tr,Arrays.asList("dest")).join();
                    Assert.assertEquals("incorrect path",Paths.get("dest"), toPathObject(boo.getPath()));
                    Assert.assertTrue("does not exist even though it's been created!", boo.exists(tr).join());

                    //move the subdirectory and see if it works
                    DirectorySubspace newBar = bar.moveTo(tr,Arrays.asList("dest","bar")).join();
                    Assert.assertEquals("incorrect path",Paths.get("dest","bar"), toPathObject(newBar.getPath()));
                    Assert.assertTrue("does not exist even though it's been created!", newBar.exists(tr).join());
                    Assert.assertFalse("Still exists in old location!",bar.exists(tr).join());

                } finally {
                    // remove the directory
                    dir.remove(tr, Arrays.asList("src")).join();
                    try{
                        dir.remove(tr, Arrays.asList("dest")).join();
                    }catch(CompletionException ce){
                        Throwable t = ce.getCause();
                        if(!(t instanceof NoSuchDirectoryException)){
                            throw ce;
                        }
                    }

                }
                return null;
            });
        }
    }

    @Test
    public void testCannotCreateDirectoryTwice() throws Exception {
        /*
         * Shouldn't be able to create the directory twice--make sure it throws the proper error if we try.
         */
        final DirectoryLayer dir = new DirectoryLayer();

        try (Database db = fdb.open()) {

            db.run(tr -> {
                List<String> path = new ArrayList<>();
                path.add("foo");
                Path correctPath = Paths.get("foo");
                try {
                    DirectorySubspace foo = dir.createOrOpen(tr, path).join();
                    Assert.assertEquals("Incorrect path", correctPath, toPathObject(foo.getPath()));
                    // make sure it exists
                    Assert.assertTrue("does not exist even though it's been created!", foo.exists(tr).join());

                    // now try to create it again
                    try {
                        DirectorySubspace foo2 = dir.create(tr, path).join();
                        Assert.fail("Was able to create a directory twice");
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
    public void testCannotRemoveNonexistingDirectory() throws Exception {
        /*
         * can't remove a directory that's not there--should throw a NoSuchDirectoryException
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

    private static Path toPathObject(List<String> pathElements) {
        String[] arr = new String[pathElements.size()];
        arr = pathElements.toArray(arr);

        String[] remaining = new String[arr.length - 1];
        for (int i = 1; i < arr.length; i++) {
            remaining[i - 1] = arr[i];
        }
        return Paths.get(arr[0], remaining);

    }

}
