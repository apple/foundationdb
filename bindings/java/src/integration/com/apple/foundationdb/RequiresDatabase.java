/*
 * RequiresDatabase.java
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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

/**
 * Rule to make it easy to write integration tests that only work when a running
 * database is detected and connectable using the default cluster file. Use this
 * as a @ClassRule on any integration test that requires a running database.
 * 
 * This will attempt to connect to an FDB instance and perform a basic operation. If it can
 * do so quickly, then it will go ahead and run the underlying test statement. If it cannot 
 * perform a basic operation against the running DB, then it will throw an AssumptionViolatedException
 * and skip running the test (JUnit treats AssumptionViolatedException as a skipped test when recording data).
 * 
 * There is a second safety valve--you can also set the env variable `run.integration.tests` to false. If it's set,
 * then all tests will just be skipped outright, without trying to connect.
 */
public class RequiresDatabase implements TestRule {
    public static RequiresDatabase require() {
        return new RequiresDatabase();
    }

    @Override
    public Statement apply(Statement base, Description description) {
        return new Statement() {

            @Override
            public void evaluate() throws Throwable {
                if (!canRunIntegrationTest()) {
                    throw new AssumptionViolatedException("Integration tests are disabled");
                }
                // attempt to connect to the database
                try (Database db = FDB.selectAPIVersion(700).open()) {
                    db.run(tr -> {
                        CompletableFuture<byte[]> future = tr.get("test".getBytes());

                        try {
                            return future.get(100, TimeUnit.MILLISECONDS);
                        } catch (TimeoutException te) {
                            throw new AssumptionViolatedException(
                                    "Cannot connect to database within timeout, skipping test");
                        } catch (InterruptedException e) {
                            throw new AssumptionViolatedException(
                                    "Interrupted during setup, skipping test");
                        } catch (ExecutionException e) {
                            throw new RuntimeException(e.getCause());
                        }
                    });
                }
                base.evaluate();
            }

        };
    }

    private static boolean canRunIntegrationTest(){
        String prop = System.getProperty("run.integration.tests");
        if(prop==null){
            return true;
        }
        return Boolean.parseBoolean(prop);
    }
    
}
