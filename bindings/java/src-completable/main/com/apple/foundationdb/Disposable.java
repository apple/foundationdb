/*
 * Disposable.java
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

package com.apple.cie.foundationdb;

/**
 * A FoundationDB object with native resources that can be freed. It is not mandatory to call
 *  {@link Disposable#dispose()} most of the time, as disposal will happen at finalization.
 */
public interface Disposable {
	/**
	 * Dispose of the object.  This can be called multiple times, but care should be
	 *  taken that an object is not in use in another thread at the time of the call.
	 */
	void dispose();
}