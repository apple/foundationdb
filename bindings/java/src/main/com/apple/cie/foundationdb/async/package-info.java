/*
 * package-info.java
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

/**
 * Provides a basic asynchronous programming library for Java.
 * <br><br>
 *
 * <h3>Concepts</h3>
 * In this library, the concepts from computer science of {@code Future}/{@code Promise}
 *  are present and named {@link com.apple.cie.foundationdb.async.Future}/{@link com.apple.cie.foundationdb.async.Settable}.
 *  As a way to deal in a reasonable manner with the presence of checked Exceptions in
 *  Java (which essentially add information to the return signature of code) this library
 *  adds the idea of {@code Partial}. Named after the mathematical concept of partial
 *  functions -- which do not have an output defined for the full range of inputs --
 *  {@link com.apple.cie.foundationdb.async.PartialFunction} are declared to throw
 *  {@link java.lang.Exception}s as some inputs will not have a valid output. Using
 *  the same names, {@link com.apple.cie.foundationdb.async.PartialFuture}s are the outputs
 *  of {@link com.apple.cie.foundationdb.async.PartialFunction}s and can have checked exceptions
 *  set as an unsuccessful output -- and those checked exceptions can be thrown from
 *  {@link com.apple.cie.foundationdb.async.PartialFuture#get()}.
 *
 * <h3>Practical Use</h3>
 * Most client use of this library should either use a blocking structure with threads
 *  calling {@link com.apple.cie.foundationdb.async.Future#get()} or use the asynchronous "mapping"
 *  functions. Use {@link com.apple.cie.foundationdb.async.Future#map(Function)} when the output
 *  of a {@code Future} needs only simple, fast transformation (e.g. for decoding small
 *  pieces of data from one format to another). Use
 *  {@link com.apple.cie.foundationdb.async.Future#flatMap(Function)} when the output will serve
 *  as the input to another longer running asynchronous process.
 *
 * <h3>Cancellation</h3>
 * {@link com.apple.cie.foundationdb.async.Future} extends the interface
 *  {@link com.apple.cie.foundationdb.async.Cancellable} and therefore can represent an operation
 *  that can be cancelled in some fashion. Cancelling a {@code Future} will cancel the
 *  process behind it, even if the {@code Future} being cancelled is a derived result
 *  of another process (see {@link com.apple.cie.foundationdb.async.Future#flatMap(Function)}).
 *  Therefore, {@code Future}s should only be cancelled when it is known that no other
 *  piece of code depends on the output of this source process. Calling
 *  {@code Future#cancel()} is the same as calling
 *  {@link com.apple.cie.foundationdb.async.Future#dispose()}.
 */
package com.apple.cie.foundationdb.async;
