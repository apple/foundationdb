/*
 * Cancellable.java
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

package com.apple.foundationdb.async;

/**
 * Describes an operation or signal that can be cancelled. Cancellation will be assumed
 *  to "chain" -- that is, once an {@code cancel()} is called, an operation will be cancelled
 *  even if there are other consumers of the operation which have not cancelled.
 *
 */
public interface Cancellable {
	/**
	 * Cancels this operation or signal. This will end the work that would have been done
	 *  and notify all consumers of the operation that a result will not be returned. It
	 *  is not an error to call this method on an operation that has already completed or
	 *  already been cancelled. This method will not block or throw non-fatal exceptions.
	 */
	void cancel();
}
