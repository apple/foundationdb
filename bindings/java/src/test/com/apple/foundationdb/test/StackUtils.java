/*
 * StackUtils.java
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

package com.apple.foundationdb.test;

import java.math.BigInteger;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.tuple.Tuple;

public class StackUtils {

	static boolean pushError(Instruction inst, FDBException error) {
		if(error.getCode() != 0)
			inst.push(getErrorBytes(error));

		return error.getCode() != 0;
	}

	static byte[] getErrorBytes(FDBException error) {
		Tuple t = new Tuple()
			.add("ERROR".getBytes())
			.add(Integer.toString(error.getCode()).getBytes());

		return t.pack();
	}

	static Object serializeFuture(Object item) {
		try {
			if(!(item instanceof CompletableFuture)) {
				return item;
			}
			CompletableFuture<?> future = (CompletableFuture<?>)item;
			item = future.join();
			if(item == null)
				item = "RESULT_NOT_PRESENT".getBytes();
		}
		catch(CompletionException e) {
			FDBException ex = getRootFDBException(e);
			if(ex == null) {
				throw e;
			}

			item = getErrorBytes(ex);
		}
		return item;
	}

	// Without a JSON parsing library, we try to validate that the metadata consists
	// of a select few properties using simple string comparison
	static boolean validTenantMetadata(String metadata) {
		return (metadata.charAt(0) == '{' && metadata.charAt(metadata.length() - 1) == '}' && metadata.contains("id") &&
		        metadata.contains("prefix"));
	}

	////////////////////////
	// Utilities for forcing Objects into various types
	////////////////////////
	static int getInt(List<Object> params, int index) {
		Object object = params.get(index);
		return getInt(object);
	}

	static int getInt(Object object) {
		return getInt(object, null);
	}

	static int getInt(Object object, Integer defaultValue) {
		if(object == null) {
			if(defaultValue == null)
				throw new NullPointerException("Null input with no default");
			return defaultValue;
		}
		return ((Number)object).intValue();
	}

	static Number getNumber(Object object) {
		return ((Number)object);
	}

	static BigInteger getBigInteger(Object object) {
		if (object instanceof BigInteger) {
			return (BigInteger)object;
		} else {
			return BigInteger.valueOf(((Number)object).longValue());
		}
	}

	static boolean getBoolean(Object o) {
		return getBoolean(o, null);
	}

	static boolean getBoolean(Object o, Boolean defaultValue) {
		if(o == null) {
			if(defaultValue == null)
				throw new NullPointerException("Null input with no default");
			return defaultValue;
		}
		return getInt(o) != 0;
	}

	static KeySelector createSelector(Object key, Object orEqualObj, Object offsetObj) {
		boolean orEqual = getBoolean(orEqualObj, null);
		int offset = getInt(offsetObj);
		return new KeySelector((byte[])key, orEqual, offset);
	}

	static FDBException getRootFDBException(Throwable t) {
		while(t != null && t != t.getCause() && !(t instanceof FDBException)){
			t = t.getCause();
		}

		return (t instanceof FDBException) ? (FDBException)t : null;
	}

	private StackUtils() {}
}
