/*
 * OptionsSet.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2024 Apple Inc. and the FoundationDB project authors
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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;

abstract class OptionsSet {
	private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");
	OptionConsumer consumer;

	OptionsSet(OptionConsumer provider) {
		this.consumer = provider;
	}

	/**
	 * Returns the object on which these options are being set. Rarely used by
	 *  client code, since all options should be top-level methods on extending
	 *  classes.
	 *
	 * @return target of option set calls
	 */
	public OptionConsumer getOptionConsumer() {
		return consumer;
	}

	protected void setOption(int code) {
		consumer.setOption(code, null);
	}

	protected void setOption(int code, byte[] param) {
		consumer.setOption(code,  param);
	}

	protected void setOption(int code, String param) {
		consumer.setOption(code, param == null ? null : param.getBytes(CHARSET_UTF8));
	}

	protected void setOption(int code, long param) {
		ByteBuffer b = ByteBuffer.allocate(8);
		b.order(ByteOrder.LITTLE_ENDIAN);
		b.putLong(param);
		consumer.setOption(code, b.array());
	}
}
