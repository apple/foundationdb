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

package com.apple.foundationdb.tuple;

import java.math.BigInteger;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import com.apple.foundationdb.FDB;

class TupleUtil {
	private static final byte nil = 0x00;
	private static final BigInteger[] size_limits;
	private static final Charset UTF8;
	private static final IterableComparator iterableComparator;

	private static final byte BYTES_CODE            = 0x01;
	private static final byte STRING_CODE           = 0x02;
	private static final byte NESTED_CODE           = 0x05;
	private static final byte INT_ZERO_CODE         = 0x14;
	private static final byte POS_INT_END           = 0x1d;
	private static final byte NEG_INT_START         = 0x0b;
	private static final byte FLOAT_CODE            = 0x20;
	private static final byte DOUBLE_CODE           = 0x21;
	private static final byte FALSE_CODE            = 0x26;
	private static final byte TRUE_CODE             = 0x27;
	private static final byte UUID_CODE             = 0x30;
	private static final byte VERSIONSTAMP_CODE     = 0x33;

	private static final byte[] NULL_ARR           = new byte[] {nil};
	private static final byte[] NULL_ESCAPED_ARR   = new byte[] {nil, (byte)0xFF};
	private static final byte[] BYTES_ARR          = new byte[]{0x01};
	private static final byte[] STRING_ARR         = new byte[]{0x02};
	private static final byte[] NESTED_ARR         = new byte[]{0x05};
	private static final byte[] FALSE_ARR          = new byte[]{0x26};
	private static final byte[] TRUE_ARR           = new byte[]{0x27};
	private static final byte[] VERSIONSTAMP_ARR   = new byte[]{0x33};

	private static final ThreadLocal<DecodeResult> DECODE_RESULT_THREAD_LOCAL =
			new ThreadLocal<DecodeResult>() {
				@Override
				protected DecodeResult initialValue() {
					return new DecodeResult();
				}
			};
	private static final ThreadLocal<EncodeResult> ENCODE_RESULT_THREAD_LOCAL =
			new ThreadLocal<EncodeResult>() {
				@Override
				protected EncodeResult initialValue() {
					return new EncodeResult();
				}
			};
	private static final ThreadLocal<byte[]> NINE_BYTES_THREAD_LOCAL =
			new ThreadLocal<byte[]>() {
				@Override
				protected byte[] initialValue() {
					return new byte[9];
				}
			};

	static {
		size_limits = new BigInteger[9];
		for(int i = 0; i < 9; i++) {
			size_limits[i] = (BigInteger.ONE).shiftLeft(i * 8).subtract(BigInteger.ONE);
		}
		UTF8 = Charset.forName("UTF-8");
		iterableComparator = new IterableComparator();
	}

	static class DecodeResult {
		int end;
		Object o;

		DecodeResult() {
		}

		DecodeResult set(int end, Object o) {
			this.end = end;
			this.o = o;
			return this;
		}
	}

	static class EncodeResult {
		int totalLength;
		int versionPos;

		EncodeResult() {
		}

		EncodeResult set(int totalLength, int versionPos) {
			this.totalLength = totalLength;
			this.versionPos = versionPos;
			return this;
		}
	}

	static int byteLength(byte[] bytes) {
		for(int i = 0; i < bytes.length; i++) {
			if(bytes[i] == 0x00) continue;
			return bytes.length - i;
		}
		return 0;
	}

	/**
	 * Takes the Big-Endian byte representation of a floating point number and adjusts
	 * it so that it sorts correctly. For encoding, if the sign bit is 1 (the number
	 * is negative), then we need to flip all of the bits; otherwise, just flip the
	 * sign bit. For decoding, if the sign bit is 0 (the number is negative), then
	 * we also need to flip all of the bits; otherwise, just flip the sign bit.
	 * This will mutate in place the given array.
	 *
	 * @param bytes Big-Endian IEEE encoding of a floating point number
	 * @param start the (zero-indexed) first byte in the array to mutate
	 * @param encode <code>true</code> if we encoding the float and <code>false</code> if we are decoding
	 * @return the encoded {@code byte[]}
	 */
	static byte[] floatingPointCoding(byte[] bytes, int start, boolean encode) {
		if(encode && (bytes[start] & (byte)0x80) != (byte)0x00) {
			for(int i = start; i < bytes.length; i++) {
				bytes[i] = (byte) (bytes[i] ^ 0xff);
			}
		} else if(!encode && (bytes[start] & (byte)0x80) != (byte)0x80) {
			for(int i = start; i < bytes.length; i++) {
				bytes[i] = (byte) (bytes[i] ^ 0xff);
			}
		} else {
			bytes[start] = (byte) (0x80 ^ bytes[start]);
		}

		return bytes;
	}

	public static byte[] join(List<byte[]> items) {
		return ByteArrayUtil.join(null, items);
	}

	static int getCodeFor(Object o) {
		if(o == null)
			return nil;
		if(o instanceof byte[])
			return BYTES_CODE;
		if(o instanceof String)
			return STRING_CODE;
		if(o instanceof Float)
			return FLOAT_CODE;
		if(o instanceof Double)
			return DOUBLE_CODE;
		if(o instanceof Boolean)
			return FALSE_CODE;
		if(o instanceof UUID)
			return UUID_CODE;
		if(o instanceof Number)
			return INT_ZERO_CODE;
		if(o instanceof Versionstamp)
			return VERSIONSTAMP_CODE;
		if(o instanceof List<?>)
			return NESTED_CODE;
		if(o instanceof Tuple)
			return NESTED_CODE;
		throw new IllegalArgumentException("Unsupported data type: " + o.getClass().getName());
	}

	static EncodeResult encode(Object t, boolean nested, ByteArrayOutputStream baos) throws IOException {
		if(t == null) {
			if(nested) {
				baos.write(NULL_ESCAPED_ARR);
				return ENCODE_RESULT_THREAD_LOCAL.get().set(NULL_ESCAPED_ARR.length, -1);
			}
			else {
				baos.write(NULL_ARR);
				return ENCODE_RESULT_THREAD_LOCAL.get().set(NULL_ARR.length, -1);
			}
		}
		if(t instanceof byte[])
			return encode((byte[]) t, baos);
		if(t instanceof String)
			return encode((String)t, baos);
		if(t instanceof BigInteger)
			return encode((BigInteger)t, baos);
		if(t instanceof Float)
			return encode((Float)t, baos);
		if(t instanceof Double)
			return encode((Double)t, baos);
		if(t instanceof Boolean)
			return encode((Boolean)t, baos);
		if(t instanceof UUID)
			return encode((UUID)t, baos);
		if(t instanceof Number)
			return encode(((Number)t).longValue(), baos);
		if(t instanceof Versionstamp)
			return encode((Versionstamp)t, baos);
		if(t instanceof List<?>)
			return encode((List<?>)t, baos);
		if(t instanceof Tuple)
			return encode(((Tuple)t).getItems(), baos);
		throw new IllegalArgumentException("Unsupported data type: " + t.getClass().getName());
	}

	static EncodeResult encode(Object t, ByteArrayOutputStream baos) throws IOException {
		return encode(t, false, baos);
	}

	static EncodeResult encode(byte[] bytes, ByteArrayOutputStream baos) throws IOException {
		final int length = baos.size();
		baos.write(BYTES_ARR);
		replace(baos, bytes, NULL_ARR, NULL_ESCAPED_ARR);
		baos.write(NULL_ARR);
		//System.out.println("Joining bytes...");
		return ENCODE_RESULT_THREAD_LOCAL.get().set(baos.size() - length, -1);
	}

	static EncodeResult encode(String s, ByteArrayOutputStream baos) throws IOException {
		final int length = baos.size();
		baos.write(STRING_ARR);
		replace(baos, s.getBytes(UTF8), NULL_ARR, NULL_ESCAPED_ARR);
		baos.write(NULL_ARR);
		//System.out.println("Joining string...");
		return ENCODE_RESULT_THREAD_LOCAL.get().set(baos.size() - length, -1);
	}

	static EncodeResult encode(BigInteger i, ByteArrayOutputStream baos) throws IOException {
		//System.out.println("Encoding integral " + i);
		if(i.equals(BigInteger.ZERO)) {
			baos.write(new byte[]{INT_ZERO_CODE});
			return ENCODE_RESULT_THREAD_LOCAL.get().set(1, -1);
		}
		byte[] bytes = i.toByteArray();
		final int baosLength = baos.size();
		if(i.compareTo(BigInteger.ZERO) > 0) {
			if(i.compareTo(size_limits[size_limits.length-1]) > 0) {
				int length = byteLength(bytes);
				if(length > 0xff) {
					throw new IllegalArgumentException("BigInteger magnitude is too large (more than 255 bytes)");
				}

				baos.write(POS_INT_END);
				baos.write((byte)length);
				baos.write(bytes, bytes.length - length, length);
				return ENCODE_RESULT_THREAD_LOCAL.get().set(baos.size() - baosLength, -1);
			}
			int n = ByteArrayUtil.bisectLeft(size_limits, i);
			assert n <= size_limits.length;
			//byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(i).array();
			//System.out.println("  -- integral has 'n' of " + n + " and output bytes of " + bytes.length);
			baos.write((byte)(INT_ZERO_CODE + n));
			baos.write(bytes, bytes.length - n, n);
			return ENCODE_RESULT_THREAD_LOCAL.get().set(baos.size() - baosLength, -1);
		}
		if(i.negate().compareTo(size_limits[size_limits.length-1]) > 0) {
			int length = byteLength(i.negate().toByteArray());
			if(length > 0xff) {
				throw new IllegalArgumentException("BigInteger magnitude is too large (more than 255 bytes)");
			}
			BigInteger offset = BigInteger.ONE.shiftLeft(length*8).subtract(BigInteger.ONE);
			byte[] adjusted = i.add(offset).toByteArray();

			baos.write(NEG_INT_START);
			baos.write((byte)(length ^ 0xff));
			if(adjusted.length >= length) {
				baos.write(adjusted, adjusted.length - length, length);
			} else {
				for (int j = 0; j < length + 2 - adjusted.length; ++j) {
					baos.write(nil);
				}
				baos.write(adjusted, 0, adjusted.length);
			}
			return ENCODE_RESULT_THREAD_LOCAL.get().set(baos.size() - baosLength, -1);
		}
		int n = ByteArrayUtil.bisectLeft(size_limits, i.negate());

		assert n >= 0 && n < size_limits.length; // can we do this? it seems to be required for the following statement

		long maxv = size_limits[n].add(i).longValue();
		byte[] adjustedBytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(maxv).array();

		baos.write((byte)(20 - n));
		baos.write(adjustedBytes, adjustedBytes.length - n, n);
		return ENCODE_RESULT_THREAD_LOCAL.get().set(baos.size() - baosLength, -1);
	}

	static EncodeResult encode(Integer i, ByteArrayOutputStream baos) throws IOException {
		return encode(i.longValue(), baos);
	}

	static EncodeResult encode(long i, ByteArrayOutputStream baos) throws IOException {
		return encode(BigInteger.valueOf(i), baos);
	}

	static EncodeResult encode(Float f, ByteArrayOutputStream baos) throws IOException {
		byte[] result = ByteBuffer.allocate(5).order(ByteOrder.BIG_ENDIAN).put(FLOAT_CODE).putFloat(f).array();
		floatingPointCoding(result, 1, true);
		baos.write(result);
		return ENCODE_RESULT_THREAD_LOCAL.get().set(result.length, -1);
	}

	static EncodeResult encode(Double d, ByteArrayOutputStream baos) throws IOException {
		byte[] result = ByteBuffer.allocate(9).order(ByteOrder.BIG_ENDIAN).put(DOUBLE_CODE).putDouble(d).array();
		floatingPointCoding(result, 1, true);
		baos.write(result);
		return ENCODE_RESULT_THREAD_LOCAL.get().set(result.length, -1);
	}

	static EncodeResult encode(Boolean b, ByteArrayOutputStream baos) throws IOException {
		if (b) {
			baos.write(TRUE_ARR);
		} else {
			baos.write(FALSE_ARR);
		}
		return ENCODE_RESULT_THREAD_LOCAL.get().set(1, -1);
	}

	static EncodeResult encode(UUID uuid, ByteArrayOutputStream baos) throws IOException {
		byte[] result = ByteBuffer.allocate(17).put(UUID_CODE).order(ByteOrder.BIG_ENDIAN)
				.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits())
				.array();
		baos.write(result);
		return ENCODE_RESULT_THREAD_LOCAL.get().set(result.length, -1);
	}

	static EncodeResult encode(Versionstamp v, ByteArrayOutputStream baos) throws IOException {
		baos.write(VERSIONSTAMP_ARR);
		baos.write(v.getBytes());
		return ENCODE_RESULT_THREAD_LOCAL.get().set(1 + Versionstamp.LENGTH, (v.isComplete() ? -1 : 1));
	}

	static EncodeResult encode(List<?> value, ByteArrayOutputStream baos) throws IOException {
		int lenSoFar = 0;
		int versionPos = -1;
		baos.write(NESTED_ARR);
		for(Object t : value) {
			EncodeResult childResult = encode(t, true, baos);
			if(childResult.versionPos > 0) {
				if(versionPos > 0) {
					throw new IllegalArgumentException("Multiple incomplete Versionstamps included in Tuple");
				}
				versionPos = lenSoFar + childResult.versionPos;
			}
			lenSoFar += childResult.totalLength;
		}
		baos.write(NULL_ARR);
		return ENCODE_RESULT_THREAD_LOCAL.get().set(lenSoFar + 2, (versionPos < 0 ? -1 : versionPos + 1));
	}

	static DecodeResult decode(byte[] rep, int pos, int last, boolean decode) {
		//System.out.println("Decoding '" + ArrayUtils.printable(rep) + "' at " + pos);

		// SOMEDAY: codes over 127 will be a problem with the signed Java byte mess
		int code = rep[pos];
		int start = pos + 1;
		if(code == nil) {
			return DECODE_RESULT_THREAD_LOCAL.get().set(start, null);
		}
		if(code == BYTES_CODE) {
			int end = ByteArrayUtil.findTerminator(rep, nil, (byte)0xff, start, last);
			if (!decode) {
				return DECODE_RESULT_THREAD_LOCAL.get().set(end + 1, null);
			}
			byte[] value = Arrays.copyOfRange(rep, start, end);
			value = replace(value, NULL_ESCAPED_ARR, NULL_ARR);
			return DECODE_RESULT_THREAD_LOCAL.get().set(end + 1, value);
		}
		if(code == STRING_CODE) {
			int end = ByteArrayUtil.findTerminator(rep, nil, (byte)0xff, start, last);
			if (!decode) {
				return DECODE_RESULT_THREAD_LOCAL.get().set(end + 1, null);
			}
			byte[] value = Arrays.copyOfRange(rep, start, end);
			value = replace(value, NULL_ESCAPED_ARR, NULL_ARR);
			String str = new String(value, UTF8);
			return DECODE_RESULT_THREAD_LOCAL.get().set(end + 1, str);
		}
		if(code == FLOAT_CODE) {
			if (!decode) {
				return DECODE_RESULT_THREAD_LOCAL.get().set(start + Float.BYTES, null);
			}
			byte[] resBytes = Arrays.copyOfRange(rep, start, start + Float.BYTES);
			floatingPointCoding(resBytes, 0, false);
			float res = ByteBuffer.wrap(resBytes).order(ByteOrder.BIG_ENDIAN).getFloat();
			return DECODE_RESULT_THREAD_LOCAL.get().set(start + Float.BYTES, res);
		}
		if(code == DOUBLE_CODE) {
			if (!decode) {
				return DECODE_RESULT_THREAD_LOCAL.get().set(start + Double.BYTES, null);
			}
			byte[] resBytes = Arrays.copyOfRange(rep, start, start + Double.BYTES);
			floatingPointCoding(resBytes, 0, false);
			double res = ByteBuffer.wrap(resBytes).order(ByteOrder.BIG_ENDIAN).getDouble();
			return DECODE_RESULT_THREAD_LOCAL.get().set(start + Double.BYTES, res);
		}
		if(code == FALSE_CODE) {
			return DECODE_RESULT_THREAD_LOCAL.get().set(start, false);
		}
		if(code == TRUE_CODE) {
			return DECODE_RESULT_THREAD_LOCAL.get().set(start, true);
		}
		if(code == UUID_CODE) {
			final int UUIDBytes = 16;
			if (!decode) {
				return DECODE_RESULT_THREAD_LOCAL.get().set(start + UUIDBytes, null);
			}
			ByteBuffer bb = ByteBuffer.wrap(rep, start, UUIDBytes).order(ByteOrder.BIG_ENDIAN);
			long msb = bb.getLong();
			long lsb = bb.getLong();
			return DECODE_RESULT_THREAD_LOCAL.get().set(start + UUIDBytes, new UUID(msb, lsb));
		}
		if(code == POS_INT_END) {
			int n = rep[start] & 0xff;
			if (!decode) {
				return DECODE_RESULT_THREAD_LOCAL.get().set(start + n + 1, null);
			}
			return DECODE_RESULT_THREAD_LOCAL.get().set(start + n + 1,
					new BigInteger(ByteArrayUtil.join(NULL_ARR, Arrays.copyOfRange(rep, start+1, start+n+1))));
		}
		if(code == NEG_INT_START) {
			int n = (rep[start] ^ 0xff) & 0xff;
			if (!decode) {
				return DECODE_RESULT_THREAD_LOCAL.get().set(start + n + 1, null);
			}
			BigInteger origValue = new BigInteger(ByteArrayUtil.join(NULL_ARR, Arrays.copyOfRange(rep, start + 1, start + n + 1)));
			BigInteger offset = BigInteger.ONE.shiftLeft(n * 8).subtract(BigInteger.ONE);
			return DECODE_RESULT_THREAD_LOCAL.get().set(start + n + 1, origValue.subtract(offset));
		}
		if(code > NEG_INT_START && code < POS_INT_END) {
			// decode a long
			boolean upper = code >= INT_ZERO_CODE;
			int n = upper ? code - 20 : 20 - code;
			int end = start + n;

			if(rep.length < end) {
				throw new RuntimeException("Invalid tuple (possible truncation)");
			}
			if (!decode) {
				return DECODE_RESULT_THREAD_LOCAL.get().set(end, 0);
			}
			byte[] longBytes = NINE_BYTES_THREAD_LOCAL.get();
			Arrays.fill(longBytes, (byte) 0);
			System.arraycopy(rep, start, longBytes, longBytes.length - n, n);
			if (!upper) {
				for (int i = longBytes.length - n; i < longBytes.length; i++) {
					longBytes[i] = (byte) (longBytes[i] ^ 0xff);
				}
			}
			BigInteger val = new BigInteger(longBytes);
			if (!upper) {
				val = val.negate();
			}

			// Convert to long if in range -- otherwise, leave as BigInteger.
			if (val.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0 ||
					val.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
				// This can occur if the thing can be represented with 8 bytes but not
				// the right sign information.
				return DECODE_RESULT_THREAD_LOCAL.get().set(end, val);
			}
			return DECODE_RESULT_THREAD_LOCAL.get().set(end, val.longValue());
		}
		if(code == VERSIONSTAMP_CODE) {
			if (!decode) {
				return DECODE_RESULT_THREAD_LOCAL.get().set(start + Versionstamp.LENGTH, null);
			}
			return DECODE_RESULT_THREAD_LOCAL.get().set(start + Versionstamp.LENGTH,
					Versionstamp.fromBytes(Arrays.copyOfRange(rep, start, start + Versionstamp.LENGTH)));
		}
		if(code == NESTED_CODE) {
			List<Object> items = new LinkedList<Object>();
			int endPos = start;
			while(endPos < rep.length) {
				if(rep[endPos] == nil) {
					if(endPos + 1 < rep.length && rep[endPos+1] == (byte)0xff) {
						items.add(null);
						endPos += 2;
					} else {
						endPos += 1;
						break;
					}
				} else {
					DecodeResult subResult = decode(rep, endPos, last, decode);
					items.add(subResult.o);
					endPos = subResult.end;
				}
			}
			return DECODE_RESULT_THREAD_LOCAL.get().set(endPos, items);
		}
		throw new IllegalArgumentException("Unknown tuple data type " + code + " at index " + pos);
	}

	static int compareSignedBigEndian(byte[] arr1, byte[] arr2) {
		if(arr1[0] < 0 && arr2[0] < 0) {
			return -1 * ByteArrayUtil.compareUnsigned(arr1, arr2);
		} else if(arr1[0] < 0) {
			return -1;
		} else if(arr2[0] < 0) {
			return 1;
		} else {
			return ByteArrayUtil.compareUnsigned(arr1, arr2);
		}
	}

	static int compareItems(Object item1, Object item2) {
		int code1 = TupleUtil.getCodeFor(item1);
		int code2 = TupleUtil.getCodeFor(item2);

		if(code1 != code2) {
			return Integer.compare(code1, code2);
		}

		if(code1 == nil) {
			// All null's are equal. (Some may be more equal than others.)
			return 0;
		}
		if(code1 == BYTES_CODE) {
			return ByteArrayUtil.compareUnsigned((byte[])item1, (byte[])item2);
		}
		if(code1 == STRING_CODE) {
			return ByteArrayUtil.compareUnsigned(((String)item1).getBytes(UTF8), ((String)item2).getBytes(UTF8));
		}
		if(code1 == INT_ZERO_CODE) {
			BigInteger bi1;
			if(item1 instanceof BigInteger) {
				bi1 = (BigInteger)item1;
			} else {
				bi1 = BigInteger.valueOf(((Number)item1).longValue());
			}
			BigInteger bi2;
			if(item2 instanceof BigInteger) {
				bi2 = (BigInteger)item2;
			} else {
				bi2 = BigInteger.valueOf(((Number)item2).longValue());
			}
			return bi1.compareTo(bi2);
		}
		if(code1 == DOUBLE_CODE) {
			// This is done over vanilla double comparison basically to handle NaN
			// sorting correctly.
			byte[] dBytes1 = ByteBuffer.allocate(8).putDouble((Double)item1).array();
			byte[] dBytes2 = ByteBuffer.allocate(8).putDouble((Double)item2).array();
			return compareSignedBigEndian(dBytes1, dBytes2);
		}
		if(code1 == FLOAT_CODE) {
			// This is done for the same reason that double comparison is done
			// that way.
			byte[] fBytes1 = ByteBuffer.allocate(4).putFloat((Float)item1).array();
			byte[] fBytes2 = ByteBuffer.allocate(4).putFloat((Float)item2).array();
			return compareSignedBigEndian(fBytes1, fBytes2);
		}
		if(code1 == FALSE_CODE) {
			return Boolean.compare((Boolean)item1, (Boolean)item2);
		}
		if(code1 == UUID_CODE) {
			// Java UUID.compareTo is signed, so we have to used the unsigned methods.
			UUID uuid1 = (UUID)item1;
			UUID uuid2 = (UUID)item2;
			int cmp1 = Long.compareUnsigned(uuid1.getMostSignificantBits(), uuid2.getMostSignificantBits());
			if(cmp1 != 0)
				return cmp1;
			return Long.compareUnsigned(uuid1.getLeastSignificantBits(), uuid2.getLeastSignificantBits());
		}
		if(code1 == VERSIONSTAMP_CODE) {
			return ((Versionstamp)item1).compareTo((Versionstamp)item2);
		}
		if(code1 == NESTED_CODE) {
			return iterableComparator.compare((Iterable<?>)item1, (Iterable<?>)item2);
		}
		throw new IllegalArgumentException("Unknown tuple data type: " + item1.getClass());
	}

	static List<Object> unpack(byte[] bytes, int start, int length) {
		List<Object> items = new LinkedList<>();
		int pos = start;
		int end = start + length;
		while(pos < end) {
			DecodeResult decoded = decode(bytes, pos, end, true);
			items.add(decoded.o);
			pos = decoded.end;
		}
		return items;
	}

	static EncodeResult encodeAll(List<Object> items, byte[] prefix, ByteArrayOutputStream baos) throws IOException {
		if(prefix != null) {
			baos.write(prefix);
		}
		int lenSoFar = (prefix == null) ? 0 : prefix.length;
		int versionPos = -1;
		for(Object t : items) {
			EncodeResult result = encode(t, baos);
			if(result.versionPos > 0) {
				if(versionPos > 0) {
					throw new IllegalArgumentException("Multiple incomplete Versionstamps included in Tuple");
				}
				versionPos = result.versionPos + lenSoFar;
			}
			lenSoFar += result.totalLength;
		}
		//System.out.println("Joining whole tuple...");
		return ENCODE_RESULT_THREAD_LOCAL.get().set(lenSoFar, versionPos);
	}

	static byte[] pack(List<Object> items, byte[] prefix) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(16 * items.size());
		EncodeResult result = null;
		try {
			result = encodeAll(items, prefix, baos);
		} catch (IOException e) {
			throw new AssertionError();
		}
		if(result.versionPos > 0) {
			throw new IllegalArgumentException("Incomplete Versionstamp included in vanilla tuple pack");
		}
		return baos.toByteArray();
	}

	static byte[] packWithVersionstamp(List<Object> items, byte[] prefix) {
		ByteArrayOutputStream baos = new ByteArrayOutputStream(16 * items.size());
		try {
			EncodeResult result = encodeAll(items, prefix, baos);
			if(result.versionPos < 0) {
				throw new IllegalArgumentException("No incomplete Versionstamp included in tuple pack with versionstamp");
			} else {
				if(result.versionPos > 0xffff) {
					throw new IllegalArgumentException("Tuple has incomplete version at position " + result.versionPos + " which is greater than the maximum " + 0xffff);
				}
				if (FDB.instance().getAPIVersion() < 520) {
					baos.write(ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort((short)result.versionPos).array());
				} else {
					baos.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(result.versionPos).array());
				}
			}
		} catch (IOException e) {
			throw new AssertionError();
		}
		return baos.toByteArray();
	}

	static boolean hasIncompleteVersionstamp(Stream<?> items) {
		return items.anyMatch(item -> {
			if(item == null) {
				return false;
			} else if(item instanceof Versionstamp) {
				return !((Versionstamp) item).isComplete();
			} else if(item instanceof Tuple) {
				return hasIncompleteVersionstamp(((Tuple) item).stream());
			} else if(item instanceof Collection<?>) {
				return hasIncompleteVersionstamp(((Collection) item).stream());
			} else {
				return false;
			}
		});
	}

	static void replace(ByteArrayOutputStream baos, byte array[], byte search[], byte replace[]) {
		int i = 0;
		int j = 0;
		while (i <= array.length - search.length) {
			if (ByteArrayUtil.regionEquals(array, i, search)) {
				baos.write(array, j, i - j);
				try {
					baos.write(replace);
				} catch (IOException e) {
					throw new AssertionError(e);
				}
				i += search.length;
				j = i;
			} else {
				i++;
			}
		}
		baos.write(array, j, array.length - j);
	}

	static byte[] replace(byte array[], byte search[], byte replace[]) {
		int i = 0;
		int j = 0;
		int k = 0;
		byte[] toReturn = null;
		while (i <= array.length - search.length) {
			if (ByteArrayUtil.regionEquals(array, i, search)) {
				if (toReturn == null) {
					double ratio = (double) replace.length / search.length;
					if (ratio < 1) ratio = 1;
					toReturn = new byte[(int) Math.ceil(array.length * ratio)];
				}
				System.arraycopy(array, j, toReturn, k, i - j);
				k += i - j;
				System.arraycopy(replace, 0, toReturn, k, replace.length);
				k += replace.length;
				i += search.length;
				j = i;
			} else {
				i++;
			}
		}
		if (toReturn == null) return array;
		System.arraycopy(array, j, toReturn, k, array.length - j);
		k += array.length - j;
		if (k != toReturn.length) {
			byte[] shrunk = new byte[k];
			System.arraycopy(toReturn, 0, shrunk, 0, k);
			return shrunk;
		}
		return toReturn;
	}

	public static void main(String[] args) {
		try {
			byte[] bytes = pack(Collections.singletonList(4), null);
			assert 4 == (Integer)(decode(bytes, 0, bytes.length, true).o);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error " + e.getMessage());
		}

		try {
			byte[] bytes = pack(Collections.singletonList("\u021Aest \u0218tring"), null);
			String string = (String)(decode(bytes, 0, bytes.length, true).o);
			System.out.println("contents -> " + string);
			assert "\u021Aest \u0218tring".equals(string);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Error " + e.getMessage());
		}

		/*Object[] a = new Object[] { "\u0000a", -2, "b\u0001", 12345, ""};
		List<Object> o = Arrays.asList(a);
		byte[] packed = pack( o, null );
		System.out.println("packed length: " + packed.length);
		o = unpack( packed, 0, packed.length );
		System.out.println("unpacked elements: " + o);
		for(Object obj : o)
			System.out.println(" -> type: " + obj.getClass().getName());*/
	}
	private TupleUtil() {}
}
