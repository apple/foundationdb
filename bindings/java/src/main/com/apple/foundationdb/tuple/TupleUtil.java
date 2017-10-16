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
import java.util.UUID;

class TupleUtil {
	private static final byte nil = 0x00;
	private static final byte[] nil_rep = new byte[] {nil, (byte)0xFF};
	private static final BigInteger[] size_limits;
	private static final Charset UTF8;
	private static final IterableComparator iterableComparator;

	private static final byte BYTES_CODE    = 0x01;
	private static final byte STRING_CODE   = 0x02;
	private static final byte NESTED_CODE   = 0x05;
	private static final byte INT_ZERO_CODE = 0x14;
	private static final byte POS_INT_END   = 0x1d;
	private static final byte NEG_INT_START = 0x0b;
	private static final byte FLOAT_CODE    = 0x20;
	private static final byte DOUBLE_CODE   = 0x21;
	private static final byte FALSE_CODE    = 0x26;
	private static final byte TRUE_CODE     = 0x27;
	private static final byte UUID_CODE     = 0x30;

	static {
		size_limits = new BigInteger[9];
		for(int i = 0; i < 9; i++) {
			size_limits[i] = (BigInteger.ONE).shiftLeft(i * 8).subtract(BigInteger.ONE);
		}
		UTF8 = Charset.forName("UTF-8");
		iterableComparator = new IterableComparator();
	}

	static class DecodeResult {
		final int end;
		final Object o;

		DecodeResult(int pos, Object o) {
			this.end = pos;
			this.o = o;
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
		if(o instanceof List<?>)
			return NESTED_CODE;
		if(o instanceof Tuple)
			return NESTED_CODE;
		throw new IllegalArgumentException("Unsupported data type: " + o.getClass().getName());
	}

	static byte[] encode(Object t, boolean nested) {
		if(t == null)
			if (nested)
				return new byte[]{nil, (byte) 0xff};
			else
				return new byte[]{nil};
		if(t instanceof byte[])
			return encode((byte[])t);
		if(t instanceof String)
			return encode((String)t);
		if(t instanceof BigInteger)
			return encode((BigInteger)t);
		if(t instanceof Float)
			return encode((Float)t);
		if(t instanceof Double)
			return encode((Double)t);
		if(t instanceof Boolean)
			return encode((Boolean)t);
		if(t instanceof UUID)
			return encode((UUID)t);
		if(t instanceof Number)
			return encode(((Number)t).longValue());
		if(t instanceof List<?>)
			return encode((List<?>)t);
		if(t instanceof Tuple)
			return encode(((Tuple)t).getItems());
		throw new IllegalArgumentException("Unsupported data type: " + t.getClass().getName());
	}

	static byte[] encode(Object t) {
		return encode(t, false);
	}

	static byte[] encode(byte[] bytes) {
		List<byte[]> list = new ArrayList<byte[]>(3);
		list.add(new byte[] {BYTES_CODE});
		list.add(ByteArrayUtil.replace(bytes, new byte[] {nil}, nil_rep));
		list.add(new byte[] {nil});

		//System.out.println("Joining bytes...");
		return ByteArrayUtil.join(null, list);
	}

	static byte[] encode(String s) {
		List<byte[]> list = new ArrayList<byte[]>(3);
		list.add(new byte[] {STRING_CODE});
		list.add(ByteArrayUtil.replace(s.getBytes(UTF8), new byte[] {nil}, nil_rep));
		list.add(new byte[] {nil});

		//System.out.println("Joining string...");
		return ByteArrayUtil.join(null, list);
	}

	static byte[] encode(BigInteger i) {
		//System.out.println("Encoding integral " + i);
		if(i.equals(BigInteger.ZERO)) {
			return new byte[] { INT_ZERO_CODE };
		}
		byte[] bytes = i.toByteArray();
		if(i.compareTo(BigInteger.ZERO) > 0) {
			if(i.compareTo(size_limits[size_limits.length-1]) > 0) {
				int length = byteLength(bytes);
				if(length > 0xff) {
					throw new IllegalArgumentException("BigInteger magnitude is too large (more than 255 bytes)");
				}
				byte[] result = new byte[length + 2];
				result[0] = POS_INT_END;
				result[1] = (byte)(length);
				System.arraycopy(bytes, bytes.length - length, result, 2, length);
				return result;
			}
			int n = ByteArrayUtil.bisectLeft(size_limits, i);
			assert n <= size_limits.length;
			//byte[] bytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(i).array();
			//System.out.println("  -- integral has 'n' of " + n + " and output bytes of " + bytes.length);
			byte[] result = new byte[n+1];
			result[0] = (byte)(INT_ZERO_CODE + n);
			System.arraycopy(bytes, bytes.length - n, result, 1, n);
			return result;
		}
		if(i.negate().compareTo(size_limits[size_limits.length-1]) > 0) {
			int length = byteLength(i.negate().toByteArray());
			if(length > 0xff) {
				throw new IllegalArgumentException("BigInteger magnitude is too large (more than 255 bytes)");
			}
			BigInteger offset = BigInteger.ONE.shiftLeft(length*8).subtract(BigInteger.ONE);
			byte[] adjusted = i.add(offset).toByteArray();
			byte[] result = new byte[length + 2];
			result[0] = NEG_INT_START;
			result[1] = (byte)(length ^ 0xff);
			if(adjusted.length >= length) {
				System.arraycopy(adjusted, adjusted.length - length, result, 2, length);
			} else {
				Arrays.fill(result, 2, result.length - adjusted.length, (byte)0x00);
				System.arraycopy(adjusted, 0, result, result.length - adjusted.length, adjusted.length);
			}
			return result;
		}
		int n = ByteArrayUtil.bisectLeft(size_limits, i.negate());

		assert n >= 0 && n < size_limits.length; // can we do this? it seems to be required for the following statement

		long maxv = size_limits[n].add(i).longValue();
		byte[] adjustedBytes = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN).putLong(maxv).array();
		byte[] result = new byte[n+1];
		result[0] = (byte)(20 - n);
		System.arraycopy(adjustedBytes, adjustedBytes.length - n, result, 1, n);
		return result;
	}

	static byte[] encode(Integer i) {
		return encode(i.longValue());
	}

	static byte[] encode(long i) {
		return encode(BigInteger.valueOf(i));
	}

	static byte[] encode(Float f) {
		byte[] result = ByteBuffer.allocate(5).order(ByteOrder.BIG_ENDIAN).put(FLOAT_CODE).putFloat(f).array();
		floatingPointCoding(result, 1, true);
		return result;
	}

	static byte[] encode(Double d) {
		byte[] result = ByteBuffer.allocate(9).order(ByteOrder.BIG_ENDIAN).put(DOUBLE_CODE).putDouble(d).array();
		floatingPointCoding(result, 1, true);
		return result;
	}

	static byte[] encode(Boolean b) {
		if (b) {
			return new byte[] {TRUE_CODE};
		} else {
			return new byte[] {FALSE_CODE};
		}
	}

	static byte[] encode(UUID uuid) {
		return ByteBuffer.allocate(17).put(UUID_CODE).order(ByteOrder.BIG_ENDIAN)
				.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits())
				.array();
	}

	static byte[] encode(List<?> value) {
		List<byte[]> parts = new LinkedList<byte[]>();
		parts.add(new byte[]{NESTED_CODE});
		for(Object t : value) {
			parts.add(encode(t, true));
		}
		parts.add(new byte[]{0x00});
		return ByteArrayUtil.join(null, parts);
	}

	static DecodeResult decode(byte[] rep, int pos, int last) {
		//System.out.println("Decoding '" + ArrayUtils.printable(rep) + "' at " + pos);

		// SOMEDAY: codes over 127 will be a problem with the signed Java byte mess
		int code = rep[pos];
		int start = pos + 1;
		if(code == nil) {
			return new DecodeResult(start, null);
		}
		if(code == BYTES_CODE) {
			int end = ByteArrayUtil.findTerminator(rep, (byte)0x0, (byte)0xff, start, last);
			//System.out.println("End of byte string: " + end);
			byte[] range = ByteArrayUtil.replace(rep, start, end - start, nil_rep, new byte[] { nil });
			//System.out.println(" -> byte string contents: '" + ArrayUtils.printable(range) + "'");
			return new DecodeResult(end + 1, range);
		}
		if(code == STRING_CODE) {
			int end = ByteArrayUtil.findTerminator(rep, (byte)0x0, (byte)0xff, start, last);
			//System.out.println("End of UTF8 string: " + end);
			byte[] stringBytes = ByteArrayUtil.replace(rep, start, end - start, nil_rep, new byte[] { nil });
			String str = new String(stringBytes, UTF8);
			//System.out.println(" -> UTF8 string contents: '" + str + "'");
			return new DecodeResult(end + 1, str);
		}
		if(code == FLOAT_CODE) {
			byte[] resBytes = Arrays.copyOfRange(rep, start, start+4);
			floatingPointCoding(resBytes, 0, false);
			float res = ByteBuffer.wrap(resBytes).order(ByteOrder.BIG_ENDIAN).getFloat();
			return new DecodeResult(start + 4, res);
		}
		if(code == DOUBLE_CODE) {
			byte[] resBytes = Arrays.copyOfRange(rep, start, start+8);
			floatingPointCoding(resBytes, 0, false);
			double res = ByteBuffer.wrap(resBytes).order(ByteOrder.BIG_ENDIAN).getDouble();
			return new DecodeResult(start + 8, res);
		}
		if(code == FALSE_CODE) {
			return new DecodeResult(start, false);
		}
		if(code == TRUE_CODE) {
			return new DecodeResult(start, true);
		}
		if(code == UUID_CODE) {
			ByteBuffer bb = ByteBuffer.wrap(rep, start, 16).order(ByteOrder.BIG_ENDIAN);
			long msb = bb.getLong();
			long lsb = bb.getLong();
			return new DecodeResult(start + 16, new UUID(msb, lsb));
		}
		if(code == POS_INT_END) {
			int n = rep[start] & 0xff;
			return new DecodeResult(start + n + 1, new BigInteger(ByteArrayUtil.join(new byte[]{0x00}, Arrays.copyOfRange(rep, start+1, start+n+1))));
		}
		if(code == NEG_INT_START) {
			int n = (rep[start] ^ 0xff) & 0xff;
			BigInteger origValue = new BigInteger(ByteArrayUtil.join(new byte[]{0x00}, Arrays.copyOfRange(rep, start+1, start+n+1)));
			BigInteger offset = BigInteger.ONE.shiftLeft(n*8).subtract(BigInteger.ONE);
			return new DecodeResult(start + n + 1, origValue.subtract(offset));
		}
		if(code > NEG_INT_START && code < POS_INT_END) {
			// decode a long
			byte[] longBytes = new byte[9];
			boolean upper = code >= INT_ZERO_CODE;
			int n = upper ? code - 20 : 20 - code;
			int end = start + n;

			if(rep.length < end) {
				throw new RuntimeException("Invalid tuple (possible truncation)");
			}

			System.arraycopy(rep, start, longBytes, longBytes.length-n, n);
			if (!upper)
				for(int i=longBytes.length-n; i<longBytes.length; i++)
					longBytes[i] = (byte)(longBytes[i] ^ 0xff);

			BigInteger val = new BigInteger(longBytes);
			if (!upper) val = val.negate();

			// Convert to long if in range -- otherwise, leave as BigInteger.
			if (val.compareTo(BigInteger.valueOf(Long.MIN_VALUE))<0||
					val.compareTo(BigInteger.valueOf(Long.MAX_VALUE))>0) {
					// This can occur if the thing can be represented with 8 bytes but not
					// the right sign information.
					return new DecodeResult(end, val);
			}
			return new DecodeResult(end, val.longValue());
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
					DecodeResult subResult = decode(rep, endPos, last);
					items.add(subResult.o);
					endPos = subResult.end;
				}
			}
			return new DecodeResult(endPos, items);
		}
		throw new IllegalArgumentException("Unknown tuple data type " + code + " at index " + pos);
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
			byte[] encoded1 = encode((Double)item1);
			byte[] encoded2 = encode((Double)item2);
			return ByteArrayUtil.compareUnsigned(encoded1, encoded2);
		}
		if(code1 == FLOAT_CODE) {
			// This is done for the same reason that double comparison is done
			// that way.
			byte[] encoded1 = encode((Float)item1);
			byte[] encoded2 = encode((Float)item2);
			return ByteArrayUtil.compareUnsigned(encoded1, encoded2);
		}
		if(code1 == FALSE_CODE) {
			return Boolean.compare((Boolean)item1, (Boolean)item2);
		}
		if(code1 == UUID_CODE) {
			// Java UUID.compareTo is signed.
			byte[] encoded1 = encode((UUID)item1);
			byte[] encoded2 = encode((UUID)item2);
			return ByteArrayUtil.compareUnsigned(encoded1, encoded2);
		}
		if(code1 == NESTED_CODE) {
			return iterableComparator.compare((Iterable<?>)item1, (Iterable<?>)item2);
		}
		throw new IllegalArgumentException("Unknown tuple data type: " + item1.getClass());
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
