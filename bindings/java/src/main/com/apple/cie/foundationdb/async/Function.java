/*
 * Function.java
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

package com.apple.cie.foundationdb.async;

/**
 * Applies a process to an input with typed output. The function itself should return
 *  immediately and, if a blocking process is needed, launch that process and return
 *  a {@link Future} handle. If the code implementing {@link #apply(Object) apply} throws
 *  a checked exception use {@link PartialFunction} instead.
 *
 * @see PartialFunction
 * @see Future
 *
 * @param <T> the type of the input variable
 * @param <V> the type of the output
 */
public interface Function<T, V> extends PartialFunction<T, V> {
	/**
	 * Applies a process on input {@code o} to obtain a result or type {@code V}. A
	 *  special case of {@link PartialFunction#apply(Object)} that does not throw
	 *  checked exceptions.
	 *
	 * @param o the input to process
	 *
	 * @return a result of type {@code V}
	 */
	@Override
	public V apply(T o);
}
