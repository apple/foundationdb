/*
 * TupleUtil.java
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

package com.apple.cie.foundationdb.tuple;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

class TupleUtil {
	private static final byte nil = 0x0;
	private static final byte[] nil_rep = new byte[] {nil, (byte)0xFF};
	private static final BigInteger[] size_limits;
	private static final Charset UTF8;

	static {
		size_limits = new BigInteger[9];
		for(int i = 0; i < 9; i++) {
			size_limits[i] = (BigInteger.ONE).shiftLeft(i * 8).subtract(BigInteger.ONE);
		}
		UTF8 = Charset.forName("UTF-8");
	}

	static class DecodeResult {
		final int end;
		final Object o;

		DecodeResult(int pos, Object o) {
			this.end = pos;
			this.o = o;
		}
	}

	public static byte[] join(List<byte[]> items) {
		return ByteArrayUtil.join(null, items);
	}

	static byte[] encode(Object t) {
		if(t == null)
			return new byte[] {nil};
		if(t instanceof byte[])
			return encode((byte[])t);
		if(t instanceof String)
			return encode((String)t);
		if(t instanceof Number)
			return encode(((Number)t).longValue());
		throw new IllegalArgumentException("Unsupported data type: " + t.getClass().getName());
	}

	static byte[] encode(byte[] bytes) {
		List<byte[]> list = new ArrayList<byte[]>(3);
		list.add(new byte[] {0x1});
		list.add(ByteArrayUtil.replace(bytes, new byte[] {0x0}, nil_rep));
		list.add(new byte[] {0x0});

		//System.out.println("Joining bytes...");
		return ByteArrayUtil.join(null, list);
	}

	static byte[] encode(String s) {
		List<byte[]> list = new ArrayList<byte[]>(3);
		list.add(new byte[] {0x2});
		list.add(ByteArrayUtil.replace(s.getBytes(UTF8), new byte[] {0x0}, nil_rep));
		list.add(new byte[] {0x0});

		//System.out.println("Joining string...");
		return ByteArrayUtil.join(null, list);
	}

	static byte[] encode(long i) {
		//System.out.println("Encoding integral " + i);
		if(i == 0) {
			return new byte[] { 20 };
		}
		if(i > 0) {
			int n = ByteArrayUtil.bisectLeft(size_limits, BigInteger.valueOf(i));
			assert n <= size_limits.length;
			byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(i).array();
			//System.out.println("  -- integral has 'n' of " + n + " and output bytes of " + bytes.length);
			byte[] result = new byte[n+1];
			result[0] = (byte)(20 + n);
			System.arraycopy(bytes, bytes.length - n, result, 1, n);
			return result;
		}
		BigInteger bI = BigInteger.valueOf(i);
		int n = ByteArrayUtil.bisectLeft(size_limits, bI.negate());

		assert n >= 0 && n < size_limits.length; // can we do this? it seems to be required for the following statement

		long maxv = size_limits[n].add(bI).longValue();
		byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(maxv).array();
		byte[] result = new byte[n+1];
		result[0] = (byte)(20 - n);
		System.arraycopy(bytes, bytes.length - n, result, 1, n);
		return result;
	}

	static byte[] encode(Integer i) {
		return encode(i.longValue());
	}

	static DecodeResult decode(byte[] rep, int pos, int last) {
		//System.out.println("Decoding '" + ArrayUtils.printable(rep) + "' at " + pos);

		// SOMEDAY: codes over 127 will be a problem with the signed Java byte mess
		int code = rep[pos];
		int start = pos + 1;
		if(code == 0x0) {
			return new DecodeResult(start, null);
		}
		if(code == 0x1) {
			int end = ByteArrayUtil.findTerminator(rep, (byte)0x0, (byte)0xff, start, last);
			//System.out.println("End of byte string: " + end);
			byte[] range = ByteArrayUtil.replace(rep, start, end - start, nil_rep, new byte[] { nil });
			//System.out.println(" -> byte string contents: '" + ArrayUtils.printable(range) + "'");
			return new DecodeResult(end + 1, range);
		}
		if(code == 0x2) {
			int end = ByteArrayUtil.findTerminator(rep, (byte)0x0, (byte)0xff, start, last);
			//System.out.println("End of UTF8 string: " + end);
			byte[] stringBytes = ByteArrayUtil.replace(rep, start, end - start, nil_rep, new byte[] { nil });
			String str = new String(stringBytes, UTF8);
			//System.out.println(" -> UTF8 string contents: '" + str + "'");
			return new DecodeResult(end + 1, str);
		}
		if(code >=12 && code <=28) {
			// decode a long
			byte[] longBytes = new byte[9];
			Arrays.fill(longBytes, (byte)0);
			boolean upper = code >= 20;
			int n = upper ? code - 20 : 20 - code;
			int end = start + n;

			if(rep.length < end) {
				throw new RuntimeException("Invalid tuple (possible truncation)");
			}

			System.arraycopy(rep, start, longBytes, 9-n, n);
			if (!upper)
				for(int i=9-n; i<9; i++)
					longBytes[i] = (byte)~longBytes[i];

			BigInteger val = new BigInteger(longBytes);
			if (!upper) val = val.negate();

			if (val.compareTo(BigInteger.valueOf(Long.MIN_VALUE))<0 ||
				val.compareTo(BigInteger.valueOf(Long.MAX_VALUE))>0)
				throw new RuntimeException("Value out of range for type long.");

			return new DecodeResult(end, val.longValue());
		}
		throw new IllegalArgumentException("Unknown tuple data type " + code + " at index " + pos);
	}

	static List<Object> unpack(byte[] bytes, int start, int length) {
		List<Object> items = new LinkedList<Object>();
		int pos = start;
		int end = start + length;
		while(pos < bytes.length) {
			DecodeResult decoded = decode(bytes, pos, end);
			items.add(decoded.o);
			pos = decoded.end;
		}
		return items;
	}

	static byte[] pack(List<Object> items) {
		if(items.size() == 0)
	        return new byte[0];

		List<byte[]> parts = new ArrayList<byte[]>(items.size());
		for(Object t : items) {
			//System.out.println("Starting encode: " + ArrayUtils.printable((byte[])t));
			byte[] encoded = encode(t);
			//System.out.println(" encoded -> '" + ArrayUtils.printable(encoded) + "'");
			parts.add(encoded);
		}
		//System.out.println("Joining whole tuple...");
		return ByteArrayUtil.join(null, parts);
	}

	public static void main(String[] args) {
		try {
			byte[] bytes = encode( 4 );
			assert 4 == (Integer)(decode( bytes, 0, bytes.length ).o);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error " + e.getMessage());
		}

		try {
			byte[] bytes = encode( "\u021Aest \u0218tring" );
			String string = (String)(decode( bytes, 0, bytes.length ).o);
			System.out.println("contents -> " + string);
			assert "\u021Aest \u0218tring" == string;
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error " + e.getMessage());
		}

		/*Object[] a = new Object[] { "\u0000a", -2, "b\u0001", 12345, ""};
		List<Object> o = Arrays.asList(a);
		byte[] packed = pack( o );
		System.out.println("packed length: " + packed.length);
		o = unpack( packed );
		System.out.println("unpacked elements: " + packed);
		for(Object obj : o)
			System.out.println(" -> type: " + obj.getClass().getName());*/
	}
	private TupleUtil() {}
}
