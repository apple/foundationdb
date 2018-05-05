/*
 * OptionConsumer.java
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

/**
 * An object on which encoded options can be set. Rarely used outside of
 *  internal implementation.
 */
public interface OptionConsumer {
	/**
	 * Attempt to set the given option. The parameter interpretation is completely
	 *  dependent on the option code. Normally this interface should not be used
	 *  from outside this package.
	 *
	 * @param code the encoded parameter to set
	 * @param parameter the value, the range of which is dependent on the parameter {@code code}
	 */
	void setOption(int code, byte[] parameter);
}
