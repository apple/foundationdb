/*
 * WhileTrueTest.java
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

import com.apple.foundationdb.async.AsyncUtil;
import com.apple.foundationdb.async.Function;
import com.apple.foundationdb.async.Future;
import com.apple.foundationdb.async.ReadyFuture;

public class WhileTrueTest {
    private static int count;

    public static void main(String[] args) {
        // This should caused memory issues using the old implementation within the
        // completable implementation of whileTrue. This does not appear to be
        // a problem with the non-completable implementation, which was more like
        // the new implementation over there.
        // Pro tip: Run with options -Xms16m -Xmx16m -XX:+HeadDumpOnOutOfMemoryError
        count = 10000000;

        //AsyncUtil.whileTrue(v -> CompletableFuture.completedFuture(count.decrementAndGet()).thenApplyAsync(c -> c > 0)).join();
        AsyncUtil.whileTrue(new Function<Void,Future<Boolean>>() {
            @Override
            public Future<Boolean> apply(Void v) {
                count -= 1;
                return new ReadyFuture<Boolean>(count > 0);
            }
        }).get();

        System.out.println("Final value: " + count);
    }
}
