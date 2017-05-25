/*
 * StackUtils.java
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

package com.apple.cie.foundationdb.test;

import java.util.List;

import com.apple.cie.foundationdb.FDBException;
import com.apple.cie.foundationdb.KeySelector;
import com.apple.cie.foundationdb.async.Future;
import com.apple.cie.foundationdb.tuple.Tuple;

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
			if(!(item instanceof Future)) {
				return item;
			}
			Future<?> future = (Future<?>)item;
			item = future.get();
			if(item == null)
				item = "RESULT_NOT_PRESENT".getBytes();
		}
		catch(FDBException e) {
			item = getErrorBytes(e);
		}
		catch(IllegalStateException e) {
			//Java throws this instead of an FDBException for error code 2015, so we have to translate it
			if(e.getMessage().equals("Future not ready"))
				item = getErrorBytes(new FDBException("", 2015));
			else
				throw e;
		}
		return item;
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

}
