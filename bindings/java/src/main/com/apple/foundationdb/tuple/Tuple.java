/*
 * Tuple.java
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.apple.foundationdb.Range;

/**
 * Represents a set of elements that make up a sortable, typed key. This object
 *  is comparable with other {@code Tuple}s and will sort in Java in
 *  the same order in which they would sort in FoundationDB. {@code Tuple}s sort
 *  first by the first element, then by the second, etc. This makes the tuple layer
 *  ideal for building a variety of higher-level data models.<br>
 * <h2>Types</h2>
 * A {@code Tuple} can
 *  contain byte arrays ({@code byte[]}), {@link String}s, {@link Number}s, {@link UUID}s,
 *  {@code boolean}s, {@link List}s, {@link Versionstamp}s, other {@code Tuple}s, and {@code null}.
 *  {@link Float} and {@link Double} instances will be serialized as single- and double-precision
 *  numbers respectively, and {@link BigInteger}s within the range [{@code -2^2040+1},
 *  {@code 2^2040-1}] are serialized without loss of precision (those outside the range
 *  will raise an {@link IllegalArgumentException}). All other {@code Number}s will be converted to
 *  a {@code long} integral value, so the range will be constrained to
 *  [{@code -2^63}, {@code 2^63-1}]. Note that for numbers outside this range the way that Java
 *  truncates integral values may yield unexpected results.<br>
 * <h2>{@code null} values</h2>
 * The FoundationDB tuple specification has a special type-code for {@code None}; {@code nil}; or,
 *  as Java would understand it, {@code null}.
 *  The behavior of the layer in the presence of {@code null} varies by type with the intention
 *  of matching expected behavior in Java. {@code byte[]}, {@link String}s, {@link UUID}s, and
 *  nested {@link List}s and {@code Tuple}s can be {@code null},
 *  whereas numbers (e.g., {@code long}s and {@code double}s) and booleans cannot.
 *  This means that the typed getters ({@link #getBytes(int) getBytes()}, {@link #getString(int) getString()}),
 *  {@link #getUUID(int) getUUID()}, {@link #getNestedTuple(int) getNestedTuple()}, {@link #getVersionstamp(int) getVersionstamp},
 *  and {@link #getNestedList(int) getNestedList()}) will return {@code null} if the entry at that location was
 *  {@code null} and the typed adds ({@link #add(byte[])}, {@link #add(String)}, {@link #add(Versionstamp)}
 *  {@link #add(Tuple)}, and {@link #add(List)}) will accept {@code null}. The
 *  {@link #getLong(int) typed get for integers} and other typed getters, however, will throw a
 *  {@link NullPointerException} if the entry in the {@code Tuple} was {@code null} at that position.<br>
 * <br>
 * This class is not thread safe.
 */
public class Tuple implements Comparable<Tuple>, Iterable<Object> {
	private static final IterableComparator comparator = new IterableComparator();
	private static final byte[] EMPTY_BYTES = new byte[0];

	List<Object> elements;
	private byte[] packed = null;
	private int memoizedHash = 0;
	private int memoizedPackedSize = -1;
	private final boolean incompleteVersionstamp;

	private Tuple(Tuple original, Object newItem, boolean itemHasIncompleteVersionstamp) {
		this.elements = new ArrayList<>(original.elements.size() + 1);
		this.elements.addAll(original.elements);
		this.elements.add(newItem);
		incompleteVersionstamp = original.incompleteVersionstamp || itemHasIncompleteVersionstamp;
	}

	private Tuple(List<Object> elements) {
		this.elements = elements;
		incompleteVersionstamp = TupleUtil.hasIncompleteVersionstamp(elements);
	}

	/**
	 * Construct a new empty {@code Tuple}. After creation, items can be added
	 *  with calls to the variations of {@code add()}.
	 *
	 * @see #from(Object...)
	 * @see #fromBytes(byte[])
	 * @see #fromItems(Iterable)
	 */
	public Tuple() {
		elements = Collections.emptyList();
		packed = EMPTY_BYTES;
		memoizedPackedSize = 0;
		incompleteVersionstamp = false;
	}

	/**
	 * Creates a copy of this {@code Tuple} with an appended last element. The
	 * parameter is untyped but only {@link String}, {@code byte[]},
	 * {@link Number}s, {@link UUID}s, {@link Boolean}s, {@link List}s,
	 * {@code Tuple}s, and {@code null} are allowed. If an object of another type is
	 * passed, then an {@link IllegalArgumentException} is thrown.
	 *
	 * @param o the object to append. Must be {@link String}, {@code byte[]},
	 *          {@link Number}s, {@link UUID}, {@link List}, {@link Boolean}, or
	 *          {@code null}.
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple addObject(Object o) {
		if (o instanceof String) {
			return add((String) o);
		} 
		else if (o instanceof byte[]) {
			return add((byte[]) o);
		} 
		else if (o instanceof UUID) {
			return add((UUID) o);
		} 
		else if (o instanceof List<?>) {
			return add((List<?>) o);
		} 
		else if (o instanceof Tuple) {
			return add((Tuple) o);
		} 
		else if (o instanceof Boolean) {
			return add((Boolean) o);
		} 
		else if (o instanceof Versionstamp) {
			return add((Versionstamp) o);
		} 
		else if (o instanceof Number) {
			return new Tuple(this, o, false);
		} 
		else if (o == null) {
			return new Tuple(this, o, false);
		} 
		else {
			throw new IllegalArgumentException("Parameter type (" + o.getClass().getName() + ") not recognized");
		}
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code String} appended as the last element.
	 *
	 * @param s the {@code String} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(String s) {
		return new Tuple(this, s, false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code long} appended as the last element.
	 *
	 * @param l the number to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(long l) {
		return new Tuple(this, l, false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code byte} array appended as the last element.
	 *
	 * @param b the {@code byte}s to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(byte[] b) {
		return new Tuple(this, b, false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code boolean} appended as the last element.
	 *
	 * @param b the {@code boolean} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(boolean b) {
		return new Tuple(this, b, false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@link UUID} appended as the last element.
	 *
	 * @param uuid the {@link UUID} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(UUID uuid) {
		return new Tuple(this, uuid, false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@link BigInteger} appended as the last element.
	 *  As {@link Tuple}s cannot contain {@code null} numeric types, a {@link NullPointerException}
	 *  is raised if a {@code null} argument is passed.
	 *
	 * @param bi the {@link BigInteger} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(BigInteger bi) {
		if(bi == null) {
			throw new NullPointerException("Number types in Tuple cannot be null");
		}
		return new Tuple(this, bi, false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code float} appended as the last element.
	 *
	 * @param f the {@code float} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(float f) {
		return new Tuple(this, f, false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code double} appended as the last element.
	 *
	 * @param d the {@code double} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(double d) {
		return new Tuple(this, d, false);
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@link Versionstamp} object appended as the last
	 *  element.
	 *
	 * @param v the {@link Versionstamp} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(Versionstamp v) {
		return new Tuple(this, v, !v.isComplete());
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@link List} appended as the last element.
	 *  This does not add the elements individually (for that, use {@link Tuple#addAll(List) Tuple.addAll}).
	 *  This adds the list as a single element nested within the outer {@code Tuple}.
	 *
	 * @param l the {@link List} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(List<?> l) {
		return new Tuple(this, l, TupleUtil.hasIncompleteVersionstamp(l));
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code Tuple} appended as the last element.
	 *  This does not add the elements individually (for that, use {@link Tuple#addAll(Tuple) Tuple.addAll}).
	 *  This adds the list as a single element nested within the outer {@code Tuple}.
	 *
	 * @param t the {@code Tuple} to append
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(Tuple t) {
		return new Tuple(this, t, t.hasIncompleteVersionstamp());
	}

	/**
	 * Creates a copy of this {@code Tuple} with a {@code byte} array appended as the last element.
	 *
	 * @param b the {@code byte}s to append
	 * @param offset the starting index of {@code b} to add
	 * @param length the number of elements of {@code b} to copy into this {@code Tuple}
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple add(byte[] b, int offset, int length) {
		return new Tuple(this, Arrays.copyOfRange(b, offset, offset + length), false);
	}

	/**
	 * Create a copy of this {@code Tuple} with a list of items appended.
	 *
	 * @param o the list of objects to append. Elements must be {@link String}, {@code byte[]},
	 *  {@link Number}s, or {@code null}.
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple addAll(List<?> o) {
		List<Object> merged = new ArrayList<>(o.size() + this.elements.size());
		merged.addAll(this.elements);
		merged.addAll(o);
		return new Tuple(merged);
	}

	/**
	 * Create a copy of this {@code Tuple} with all elements from anther {@code Tuple} appended.
	 *
	 * @param other the {@code Tuple} whose elements should be appended
	 *
	 * @return a newly created {@code Tuple}
	 */
	public Tuple addAll(Tuple other) {
		List<Object> merged = new ArrayList<>(this.size() + other.size());
		merged.addAll(this.elements);
		merged.addAll(other.elements);
		Tuple t = new Tuple(merged);
		if(!t.hasIncompleteVersionstamp() && packed != null && other.packed != null) {
			t.packed = ByteArrayUtil.join(packed, other.packed);
		}
		if(memoizedPackedSize >= 0 && other.memoizedPackedSize >= 0) {
			t.memoizedPackedSize = memoizedPackedSize + other.memoizedPackedSize;
		}
		return t;
	}

	/**
	 * Get an encoded representation of this {@code Tuple}. Each element is encoded to
	 *  {@code byte}s and concatenated. Note that once a {@code Tuple} has been packed, its
	 *  serialized representation is stored internally so that future calls to this function
	 *  are faster than the initial call.
	 *
	 * @return a packed representation of this {@code Tuple}
	 */
	public byte[] pack() {
		return packInternal(null, true);
	}

	/**
	 * Get an encoded representation of this {@code Tuple}. Each element is encoded to
	 *  {@code byte}s and concatenated, and then the prefix supplied is prepended to
	 *  the array. Note that once a {@code Tuple} has been packed, its serialized representation
	 *  is stored internally so that future calls to this function are faster than the
	 *  initial call.
	 *
	 * @param prefix additional byte-array prefix to prepend to the packed bytes
	 * @return a packed representation of this {@code Tuple} prepended by the {@code prefix}
	 */
	public byte[] pack(byte[] prefix) {
		return packInternal(prefix, true);
	}

	byte[] packInternal(byte[] prefix, boolean copy) {
		if(hasIncompleteVersionstamp()) {
			throw new IllegalArgumentException("Incomplete Versionstamp included in vanilla tuple pack");
		}
		if(packed == null) {
			packed = TupleUtil.pack(elements, getPackedSize());
		}
		boolean hasPrefix = prefix != null && prefix.length > 0;
		if(hasPrefix) {
			return ByteArrayUtil.join(prefix, packed);
		}
		else if(copy) {
			return Arrays.copyOf(packed, packed.length);
		}
		else {
			return packed;
		}
	}

	/**
	 * Pack an encoded representation of this {@code Tuple} onto the end of the given {@link ByteBuffer}.
	 *  It is up to the caller to ensure that there is enough space allocated within the buffer
	 *  to avoid {@link java.nio.BufferOverflowException}s. The client may call {@link #getPackedSize()}
	 *  to determine how large this {@code Tuple} will be once packed in order to allocate sufficient memory.
	 *  Note that unlike {@link #pack()}, the serialized representation of this {@code Tuple} is not stored, so
	 *  calling this function multiple times with the same {@code Tuple} requires serializing the {@code Tuple}
	 *  multiple times.
	 * <br>
	 * <br>
	 * This method will throw an error if there are any incomplete {@link Versionstamp}s in this {@code Tuple}.
	 *
	 * @param dest the destination {@link ByteBuffer} for the encoded {@code Tuple}
	 */
	public void packInto(ByteBuffer dest) {
		if(hasIncompleteVersionstamp()) {
			throw new IllegalArgumentException("Incomplete Versionstamp included in vanilla tuple pack");
		}
		if(packed == null) {
			TupleUtil.pack(dest, elements);
		}
		else {
			dest.put(packed);
		}
	}

	/**
	 * Get an encoded representation of this {@code Tuple} for use with
	 *  {@link com.apple.foundationdb.MutationType#SET_VERSIONSTAMPED_KEY MutationType.SET_VERSIONSTAMPED_KEY}.
	 *  This works the same as the {@link #packWithVersionstamp(byte[]) one-paramter version of this method},
	 *  but it does not add any prefix to the array.
	 *
	 * @return a packed representation of this {@code Tuple} for use with versionstamp ops.
	 * @throws IllegalArgumentException if there is not exactly one incomplete {@link Versionstamp} included in this {@code Tuple}
	 */
	public byte[] packWithVersionstamp() {
		return packWithVersionstamp(null);
	}

	/**
	 * Get an encoded representation of this {@code Tuple} for use with
	 *  {@link com.apple.foundationdb.MutationType#SET_VERSIONSTAMPED_KEY MutationType.SET_VERSIONSTAMPED_KEY}.
	 *  There must be exactly one incomplete {@link Versionstamp} instance within this
	 *  {@code Tuple} or this will throw an {@link IllegalArgumentException}.
	 *  Each element is encoded to {@code byte}s and concatenated, the prefix
	 *  is then prepended to the array, and then the index of the packed incomplete
	 *  {@link Versionstamp} is appended as a little-endian integer. This can then be passed
	 *  as the key to
	 *  {@link com.apple.foundationdb.Transaction#mutate(com.apple.foundationdb.MutationType, byte[], byte[]) Transaction.mutate()}
	 *  with the {@code SET_VERSIONSTAMPED_KEY} {@link com.apple.foundationdb.MutationType}, and the transaction's
	 *  version will then be filled in at commit time.
	 * <br>
	 * <br>
	 * Note that once a {@code Tuple} has been packed, its serialized representation is stored internally so that
	 *  future calls to this function are faster than the initial call.
	 *
	 * @param prefix additional byte-array prefix to prepend to packed bytes.
	 * @return a packed representation of this {@code Tuple} for use with versionstamp ops.
	 * @throws IllegalArgumentException if there is not exactly one incomplete {@link Versionstamp} included in this {@code Tuple}
	 */
	public byte[] packWithVersionstamp(byte[] prefix) {
		return packWithVersionstampInternal(prefix, true);
	}

	byte[] packWithVersionstampInternal(byte[] prefix, boolean copy) {
		if(!hasIncompleteVersionstamp()) {
			throw new IllegalArgumentException("No incomplete Versionstamp included in tuple pack with versionstamp");
		}
		if(packed == null) {
			packed = TupleUtil.packWithVersionstamp(elements, getPackedSize());
		}
		boolean hasPrefix = prefix != null && prefix.length > 0;
		if(hasPrefix) {
			byte[] withPrefix = ByteArrayUtil.join(prefix, packed);
			TupleUtil.adjustVersionPosition(withPrefix, prefix.length);
			return withPrefix;
		}
		else if(copy) {
			return Arrays.copyOf(packed, packed.length);
		}
		else {
			return packed;
		}
	}

	byte[] packMaybeVersionstamp() {
		if(packed == null) {
			if(hasIncompleteVersionstamp()) {
				return packWithVersionstampInternal(null, false);
			}
			else {
				return packInternal(null, false);
			}
		}
		else {
			return packed;
		}
	}

	/**
	 * Gets the unserialized contents of this {@code Tuple}.
	 *
	 * @return the elements that make up this {@code Tuple}.
	 */
	public List<Object> getItems() {
		return new ArrayList<>(elements);
	}

	/**
	 * Gets the unserialized contents of this {@code Tuple} without copying into a new list.
	 *
	 * @return the elements that make up this {@code Tuple}.
	 */
	List<Object> getRawItems() {
		return elements;
	}

	/**
	 * Gets a {@link Stream} of the unserialized contents of this {@code Tuple}.
	 *
	 * @return a {@link Stream} of the elements that make up this {@code Tuple}.
	 */
	public Stream<Object> stream() {
		return elements.stream();
	}

	/**
	 * Gets an {@code Iterator} over the {@code Objects} in this {@code Tuple}. This {@code Iterator} is
	 *  unmodifiable and will throw an exception if {@link Iterator#remove() remove()} is called.
	 *
	 * @return an unmodifiable {@code Iterator} over the elements in the {@code Tuple}.
	 */
	@Override
	public Iterator<Object> iterator() {
		return Collections.unmodifiableList(this.elements).iterator();
	}

	/**
	 * Construct a new {@code Tuple} with elements decoded from a supplied {@code byte} array.
	 *  The passed byte array must not be {@code null}. This will throw an exception if the passed byte
	 *  array does not represent a valid {@code Tuple}. For example, this will throw an error if it
	 *  encounters an unknown type code or if there is a packed element that appears to be truncated.
	 *
	 * @param bytes encoded {@code Tuple} source
	 *
	 * @return a new {@code Tuple} constructed by deserializing the provided {@code byte} array
	 * @throws IllegalArgumentException if {@code bytes} does not represent a valid {@code Tuple}
	 */
	public static Tuple fromBytes(byte[] bytes) {
		return fromBytes(bytes, 0, bytes.length);
	}

	/**
	 * Construct a new {@code Tuple} with elements decoded from a supplied {@code byte} array.
	 *  The passed byte array must not be {@code null}. This will throw an exception if the specified slice of
	 *  the passed byte array does not represent a valid {@code Tuple}. For example, this will throw an error
	 *  if it encounters an unknown type code or if there is a packed element that appears to be truncated.
	 *
	 * @param bytes encoded {@code Tuple} source
	 * @param offset starting offset of byte array of encoded data
	 * @param length length of encoded data within the source
	 *
	 * @return a new {@code Tuple} constructed by deserializing the specified slice of the provided {@code byte} array
	 * @throws IllegalArgumentException if {@code offset} or {@code length} are negative or would exceed the size of
	 *  the array or if {@code bytes} does not represent a valid {@code Tuple}
	 */
	public static Tuple fromBytes(byte[] bytes, int offset, int length) {
		if(offset < 0 || offset > bytes.length) {
			throw new IllegalArgumentException("Invalid offset for Tuple deserialization");
		}
		if(length < 0 || offset + length > bytes.length) {
			throw new IllegalArgumentException("Invalid length for Tuple deserialization");
		}
		byte[] packed = Arrays.copyOfRange(bytes, offset, offset + length);
		Tuple t = new Tuple(TupleUtil.unpack(packed));
		t.packed = packed;
		t.memoizedPackedSize = length;
		return t;
	}

	/**
	 * Gets the number of elements in this {@code Tuple}.
	 *
	 * @return the number of elements in this {@code Tuple}
	 */
	public int size() {
		return this.elements.size();
	}

	/**
	 * Determine if this {@code Tuple} contains no elements.
	 *
	 * @return {@code true} if this {@code Tuple} contains no elements, {@code false} otherwise
	 */
	public boolean isEmpty() {
		return this.elements.isEmpty();
	}

	/**
	 * Gets an indexed item as a {@code long}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a number type.
	 *  The element at the index may not be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@code long}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Number}
	 * @throws NullPointerException if the element at {@code index} is {@code null}
	 */
	public long getLong(int index) {
		Object o = this.elements.get(index);
		if(o == null)
			throw new NullPointerException("Number types in Tuples may not be null");
		return ((Number)o).longValue();
	}

	/**
	 * Gets an indexed item as a {@code byte[]}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the tuple element is not a
	 *  {@code byte} array.
	 *
	 * @param index the location of the element to return
	 *
	 * @return the item at {@code index} as a {@code byte[]}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Number}
	 */
	public byte[] getBytes(int index) {
		Object o = this.elements.get(index);
		// Check needed, since the null may be of type "Object" and may not be casted to byte[]
		if(o == null)
			return null;
		return (byte[])o;
	}

	/**
	 * Gets an indexed item as a {@code String}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the tuple element is not of
	 *  {@code String} type.
	 *
	 * @param index the location of the element to return
	 *
	 * @return the item at {@code index} as a {@code String}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link String}
	 */
	public String getString(int index) {
		Object o = this.elements.get(index);
		// Check needed, since the null may be of type "Object" and may not be casted to byte[]
		if(o == null) {
			return null;
		}
		return (String)o;
	}

	/**
	 * Gets an indexed item as a {@link BigInteger}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the tuple element is not of
	 *  a {@code Number} type. If the underlying type is a floating point value, this
	 *  will lead to a loss of precision. The element at the index may not be {@code null}.
	 *
	 * @param index the location of the element to return
	 *
	 * @return the item at {@code index} as a {@link BigInteger}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Number}
	 */
	public BigInteger getBigInteger(int index) {
		Object o = this.elements.get(index);
		if(o == null)
			throw new NullPointerException("Number types in Tuples may not be null");
		if(o instanceof BigInteger) {
			return (BigInteger)o;
		} else {
			return BigInteger.valueOf(((Number)o).longValue());
		}
	}

	/**
	 * Gets an indexed item as a {@code float}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a number type.
	 *  The element at the index may not be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@code float}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Number}
	 */
	public float getFloat(int index) {
		Object o = this.elements.get(index);
		if(o == null)
			throw new NullPointerException("Number types in Tuples may not be null");
		return ((Number)o).floatValue();
	}

	/**
	 * Gets an indexed item as a {@code double}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a number type.
	 *  The element at the index may not be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@code double}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Number}
	 */
	public double getDouble(int index) {
		Object o = this.elements.get(index);
		if(o == null)
			throw new NullPointerException("Number types in Tuples may not be null");
		return ((Number)o).doubleValue();
	}

	/**
	 * Gets an indexed item as a {@code boolean}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a {@code Boolean}.
	 *  The element at the index may not be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@code boolean}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Boolean}
	 * @throws NullPointerException if the element at {@code index} is {@code null}
	 */
	public boolean getBoolean(int index) {
		Object o = this.elements.get(index);
		if(o == null)
			throw new NullPointerException("Boolean type in Tuples may not be null");
		return (Boolean)o;

	}

	/**
	 * Gets an indexed item as a {@link UUID}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a {@code UUID}.
	 *  The element at the index may be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@link UUID}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link UUID}
	 */
	public UUID getUUID(int index) {
		Object o = this.elements.get(index);
		if(o == null)
			return null;
		return (UUID)o;
	}

	/**
	 * Gets an indexed item as a {@link Versionstamp}. This function will not do type
	 *  conversion and so will throw a {@code ClassCastException} if the element is not
	 *  a {@code Versionstamp}. The element at the index may be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@link Versionstamp}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link Versionstamp}
	 */
	public Versionstamp getVersionstamp(int index) {
		Object o = this.elements.get(index);
		if(o == null)
			return null;
		return (Versionstamp)o;
	}

	/**
	 * Gets an indexed item as a {@link List}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a {@link List}
	 *  or {@code Tuple}. The element at the index may be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@link List}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@link List}
	 *         or a {@code Tuple}
	 */
	public List<Object> getNestedList(int index) {
		Object o = this.elements.get(index);
		if(o == null) {
			return null;
		}
		else if(o instanceof Tuple) {
			return ((Tuple)o).getItems();
		}
		else if(o instanceof List<?>) {
			return new ArrayList<>((List<?>) o);
		}
		else {
			throw new ClassCastException("Cannot convert item of type " + o.getClass() + " to list");
		}
	}

	/**
	 * Gets an indexed item as a {@link Tuple}. This function will not do type conversion
	 *  and so will throw a {@code ClassCastException} if the element is not a {@link List}
	 *  or {@code Tuple}. The element at the index may be {@code null}.
	 *
	 * @param index the location of the item to return
	 *
	 * @return the item at {@code index} as a {@link List}
	 *
	 * @throws ClassCastException if the element at {@code index} is not a {@code Tuple}
	 *         or a {@link List}
	 */
	public Tuple getNestedTuple(int index) {
		Object o = this.elements.get(index);
		if(o == null) {
			return null;
		}
		else if(o instanceof Tuple) {
			return (Tuple)o;
		}
		else if(o instanceof List<?>) {
			return Tuple.fromList((List<?>)o);
		}
		else {
			throw new ClassCastException("Cannot convert item of type " + o.getClass() + " to tuple");
		}
	}

	/**
	 * Gets an indexed item without forcing a type.
	 *
	 * @param index the index of the item to return
	 *
	 * @return an item from the list, without forcing type conversion
	 */
	public Object get(int index) {
		return this.elements.get(index);
	}

	/**
	 * Creates a new {@code Tuple} with the first item of this {@code Tuple} removed.
	 *
	 * @return a newly created {@code Tuple} without the first item of this {@code Tuple}
	 *
	 * @throws IllegalStateException if this {@code Tuple} is empty
	 */
	public Tuple popFront() {
		if(elements.isEmpty())
			throw new IllegalStateException("Tuple contains no elements");

		return new Tuple(elements.subList(1, elements.size()));
	}

	/**
	 * Creates a new {@code Tuple} with the last item of this {@code Tuple} removed.
	 *
	 * @return a newly created {@code Tuple} without the last item of this {@code Tuple}
	 *
	 * @throws IllegalStateException if this {@code Tuple} is empty
	 */
	public Tuple popBack() {
		if(elements.isEmpty())
			throw new IllegalStateException("Tuple contains no elements");

		return new Tuple(elements.subList(0, elements.size() - 1));
	}

	/**
	 * Returns a range representing all keys that encode {@code Tuple}s strictly starting
	 *  with this {@code Tuple}.
	 * <br>
	 * <br>
	 * For example:
	 * <pre>
	 *   Tuple t = Tuple.from("a", "b");
	 *   Range r = t.range();</pre>
	 * {@code r} includes all tuples ("a", "b", ...)
	 * <br>
	 * This function will throw an error if this {@code Tuple} contains an incomplete
	 *  {@link Versionstamp}.
	 *
	 * @return the range of keys containing all possible keys that have this {@code Tuple}
	 *  as a strict prefix
	 */
	public Range range() {
		return range(null);
	}

	/**
	 * Returns a range representing all keys that encode {@code Tuple}s strictly starting
	 *  with the given prefix followed by this {@code Tuple}.
	 * <br>
	 * <br>
	 * For example:
	 * <pre>
	 *   Tuple t = Tuple.from("a", "b");
	 *   Range r = t.range(Tuple.from("c").pack());</pre>
	 * {@code r} contains all tuples ("c", "a", "b", ...)
	 * <br>
	 * This function will throw an error if this {@code Tuple} contains an incomplete
	 *  {@link Versionstamp}.
	 *
	 * @param prefix a byte prefix to precede all elements in the range
	 *
	 * @return the range of keys containing all possible keys that have {@code prefix}
	 *   followed by this {@code Tuple} as a strict prefix
	 */
	public Range range(byte[] prefix) {
		if(hasIncompleteVersionstamp()) {
			throw new IllegalStateException("Tuple with incomplete versionstamp used for range");
		}
		byte[] p = packInternal(prefix, false);
		return new Range(ByteArrayUtil.join(p, new byte[] {0x0}),
				ByteArrayUtil.join(p, new byte[] {(byte)0xff}));
	}

	/**
	 * Determines if there is a {@link Versionstamp} included in this {@code Tuple} that has
	 *  not had its transaction version set. It will search through nested {@code Tuple}s
	 *  contained within this {@code Tuple}. It will not throw an error if it finds multiple
	 *  incomplete {@code Versionstamp} instances.
	 *
	 * @return whether there is at least one incomplete {@link Versionstamp} included in this
	 *  {@code Tuple}
	 */
	public boolean hasIncompleteVersionstamp() {
		return incompleteVersionstamp;
	}

	/**
	 * Get the number of bytes in the packed representation of this {@code Tuple}. This is done by summing
	 *  the serialized sizes of all of the elements of this {@code Tuple} and does not pack everything
	 *  into a single {@code Tuple}. The return value of this function is stored within this {@code Tuple}
	 *  after this function has been called so that subsequent calls on the same object are fast. This method
	 *  does not validate that there is not more than one incomplete {@link Versionstamp} in this {@code Tuple}.
	 *
	 * @return the number of bytes in the packed representation of this {@code Tuple}
	 */
	public int getPackedSize() {
		if(memoizedPackedSize < 0) {
			memoizedPackedSize = getPackedSize(false);
		}
		return memoizedPackedSize;
	}

	int getPackedSize(boolean nested) {
		if(memoizedPackedSize >= 0) {
			if(!nested) {
				return memoizedPackedSize;
			}
			int nullCount = 0;
			for(Object elem : elements) {
				if(elem == null) {
					nullCount++;
				}
			}
			return memoizedPackedSize + nullCount;
		}
		else {
			return TupleUtil.getPackedSize(elements, nested);
		}
	}

	/**
	 * Compare the byte-array representation of this {@code Tuple} against another. This method
	 *  will sort {@code Tuple}s in the same order that they would be sorted as keys in
	 *  FoundationDB. Returns a negative integer, zero, or a positive integer when this object's
	 *  byte-array representation is found to be less than, equal to, or greater than the
	 *  specified {@code Tuple}.
	 *
	 * @param t the {@code Tuple} against which to compare
	 *
	 * @return a negative integer, zero, or a positive integer when this {@code Tuple} is
	 *  less than, equal, or greater than the parameter {@code t}.
	 */
	@Override
	public int compareTo(Tuple t) {
		// If either tuple has an incomplete versionstamp, then there is a possibility that the byte order
		// is not the semantic comparison order.
		if(packed != null && t.packed != null && !hasIncompleteVersionstamp() && !t.hasIncompleteVersionstamp()) {
			return ByteArrayUtil.compareUnsigned(packed, t.packed);
		}
		else {
			return comparator.compare(elements, t.elements);
		}
	}

	/**
	 * Returns a hash code value for this {@code Tuple}. Computing the hash code is fairly expensive
	 *  as it involves packing the underlying {@code Tuple} to bytes. However, this value is memoized,
	 *  so for any given {@code Tuple}, it only needs to be computed once. This means that it is
	 *  generally safe to use {@code Tuple}s with hash-based data structures such as
	 *  {@link java.util.HashSet HashSet}s or {@link java.util.HashMap HashMap}s.
	 * {@inheritDoc}
	 *
	 * @return a hash code for this {@code Tuple} that can be used by hash tables
	 */
	@Override
	public int hashCode() {
		if(memoizedHash == 0) {
			memoizedHash = Arrays.hashCode(packMaybeVersionstamp());
		}
		return memoizedHash;
	}

	/**
	 * Tests for equality with another {@code Tuple}. If the passed object is not a {@code Tuple}
	 *  this returns false. If the object is a {@code Tuple}, this returns true if
	 *  {@link Tuple#compareTo(Tuple) compareTo()} would return {@code 0}.
	 *
	 * @return {@code true} if {@code obj} is a {@code Tuple} and their binary representation
	 *  is identical
	 */
	@Override
	public boolean equals(Object o) {
		if(o == null)
			return false;
		if(o == this)
			return true;
		if(o instanceof Tuple) {
			return compareTo((Tuple)o) == 0;
		}
		return false;
	}

	/**
	 * Returns a string representing this {@code Tuple}. This contains human-readable
	 *  representations of all of the elements of this {@code Tuple}. For most elements,
	 *  this means using that object's default string representation. For byte-arrays,
	 *  this means using {@link ByteArrayUtil#printable(byte[]) ByteArrayUtil.printable()}
	 *  to produce a byte-string where most printable ASCII code points have been
	 *  rendered as characters.
	 *
	 * @return a human-readable {@link String} representation of this {@code Tuple}
	 */
	@Override
	public String toString() {
		StringBuilder s = new StringBuilder("(");
		boolean first = true;

		for(Object o : elements) {
			if(!first) {
				s.append(", ");
			}

			first = false;
			if(o == null) {
				s.append("null");
			}
			else if(o instanceof String) {
				s.append("\"");
				s.append(o);
				s.append("\"");
			}
			else if(o instanceof byte[]) {
				s.append("b\"");
				s.append(ByteArrayUtil.printable((byte[])o));
				s.append("\"");
			}
			else {
				s.append(o);
			}
		}

		s.append(")");
		return s.toString();
	}

	/**
	 * Creates a new {@code Tuple} from a variable number of elements. The elements
	 *  must follow the type guidelines from {@link Tuple#addObject(Object) add}, and so
	 *  can only be {@link String}s, {@code byte[]}s, {@link Number}s, {@link UUID}s,
	 *  {@link Boolean}s, {@link List}s, {@code Tuple}s, or {@code null}s.
	 *
	 * @param items the elements from which to create the {@code Tuple}
	 *
	 * @return a new {@code Tuple} with the given items as its elements
	 */
	public static Tuple fromItems(Iterable<?> items) {
		if(items instanceof List<?>) {
			return Tuple.fromList((List<?>)items);
		}
		List<Object> elements = new ArrayList<>();
		for(Object o : items) {
			elements.add(o);
		}
		return new Tuple(elements);
	}

	/**
	 * Efficiently creates a new {@code Tuple} from a list of objects. The elements
	 *  must follow the type guidelines from {@link Tuple#addObject(Object) add}, and so
	 *  can only be {@link String}s, {@code byte[]}s, {@link Number}s, {@link UUID}s,
	 *  {@link Boolean}s, {@link List}s, {@code Tuple}s, or {@code null}s.
	 *
	 * @param items the elements from which to create the {@code Tuple}.
	 *
	 * @return a new {@code Tuple} with the given items as its elements
	 */
	public static Tuple fromList(List<?> items) {
		List<Object> elements = new ArrayList<>(items);
		return new Tuple(elements);
	}

	/**
	 * Efficiently creates a new {@code Tuple} from a {@link Stream} of objects. The
	 *  elements must follow the type guidelines from {@link Tuple#addObject(Object) add},
	 *  and so can only be {@link String}s, {@code byte[]}s, {@link Number}s, {@link UUID}s,
	 *  {@link Boolean}s, {@link List}s, {@code Tuple}s, or {@code null}s. Note that this
	 *  class will consume all elements from the {@link Stream}.
	 *
	 * @param items the {@link Stream} of items from which to create the {@code Tuple}
	 *
	 * @return a new {@code Tuple} with the given items as its elements
	 */
	public static Tuple fromStream(Stream<?> items) {
		return new Tuple(items.collect(Collectors.toList()));
	}

	/**
	 * Creates a new {@code Tuple} from a variable number of elements. The elements
	 *  must follow the type guidelines from {@link Tuple#addObject(Object) add}, and so
	 *  can only be {@link String}s, {@code byte[]}s, {@link Number}s, {@link UUID}s,
	 *  {@link Boolean}s, {@link List}s, {@code Tuple}s, or {@code null}s.
	 *
	 * @param items the elements from which to create the {@code Tuple}
	 *
	 * @return a new {@code Tuple} with the given items as its elements
	 */
	public static Tuple from(Object... items) {
		return new Tuple(Arrays.asList(items));
	}

	static void main(String[] args) {
		for(int i : new int[] {10, 100, 1000, 10000, 100000, 1000000}) {
			createTuple(i);
		}

		Versionstamp completeVersionstamp = Versionstamp.complete(new byte[]{0x0f, (byte)0xdb, 0x0f, (byte)0xdb, 0x0f, (byte)0xdb, 0x0f, (byte)0x0db, 0x0f, (byte)0xdb}, 15);
		Versionstamp incompleteVersionstamp = Versionstamp.incomplete(20);

		Tuple t = new Tuple();
		t = t.add(Long.MAX_VALUE);
		t = t.add(Long.MAX_VALUE - 1);
		t = t.add(Long.MAX_VALUE - 2);
		t = t.add(1);
		t = t.add(0);
		t = t.add(-1);
		t = t.add(Long.MIN_VALUE + 2);
		t = t.add(Long.MIN_VALUE + 1);
		t = t.add(Long.MIN_VALUE);
		t = t.add("foo");
		t = t.addObject(null);
		t = t.add(false);
		t = t.add(true);
		t = t.add(3.14159);
		t = t.add(3.14159f);
		t = t.add(java.util.UUID.randomUUID());
		t = t.add(t.getItems());
		t = t.add(t);
		t = t.add(new BigInteger("100000000000000000000000000000000000000000000"));
		t = t.add(new BigInteger("-100000000000000000000000000000000000000000000"));
		t = t.add(completeVersionstamp);
		byte[] bytes = t.pack();
		System.out.println("Packed: " + ByteArrayUtil.printable(bytes));
		List<Object> items = Tuple.fromBytes(bytes).getItems();
		for(Object obj : items) {
			if (obj != null)
				System.out.println(" -> type: (" + obj.getClass().getName() + "): " + obj);
			else
				System.out.println(" -> type: (null): null");
		}


		t = Tuple.fromStream(t.stream().map(item -> {
			if(item instanceof String) {
				return ((String)item).toUpperCase();
			} else {
				return item;
			}
		}));
		System.out.println("Upper cased: " + t);

		Tuple t2 = Tuple.fromBytes(bytes);
		System.out.println("t2.getLong(0): " + t2.getLong(0));
		System.out.println("t2.getBigInteger(1): " + t2.getBigInteger(1));
		System.out.println("t2.getString(9): " + t2.getString(9));
		System.out.println("t2.get(10): " + t2.get(10));
		System.out.println("t2.getBoolean(11): " + t2.getBoolean(11));
		System.out.println("t2.getBoolean(12): " + t2.getBoolean(12));
		System.out.println("t2.getDouble(13): " + t2.getDouble(13));
		System.out.println("t2.getFloat(13): " + t2.getFloat(13));
		System.out.println("t2.getLong(13): " + t2.getLong(13));
		System.out.println("t2.getBigInteger(13): " + t2.getBigInteger(13));
		System.out.println("t2.getDouble(14): " + t2.getDouble(14));
		System.out.println("t2.getFloat(14): " + t2.getFloat(14));
		System.out.println("t2.getLong(14): " + t2.getLong(14));
		System.out.println("t2.getBigInteger(14): " + t2.getBigInteger(14));
		System.out.println("t2.getNestedList(17): " + t2.getNestedList(17));
		System.out.println("t2.getNestedTuple(17): " + t2.getNestedTuple(17));
		System.out.println("t2.getVersionstamp(20): " + t2.getVersionstamp(20));

		int currOffset = 0;
		for (Object item : t) {
			int length = Tuple.from(item).pack().length;
			Tuple t3 = Tuple.fromBytes(bytes, currOffset, length);
			System.out.println("item = " + t3);
			currOffset += length;
		}

		System.out.println("(2*(Long.MAX_VALUE+1),) = " + ByteArrayUtil.printable(Tuple.from(
				BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE).shiftLeft(1)
		).pack()));
		System.out.println("(2*Long.MIN_VALUE,) = " + ByteArrayUtil.printable(Tuple.from(
				BigInteger.valueOf(Long.MIN_VALUE).multiply(new BigInteger("2"))
		).pack()));
		System.out.println("2*Long.MIN_VALUE = " + Tuple.fromBytes(Tuple.from(
				BigInteger.valueOf(Long.MIN_VALUE).multiply(new BigInteger("2"))).pack()).getBigInteger(0));

		Tuple vt1 = Tuple.from("complete:", completeVersionstamp, 1);
		System.out.println("vt1: " + vt1 + " ; has incomplete: " + vt1.hasIncompleteVersionstamp());
		Tuple vt2 = Tuple.from("incomplete:", incompleteVersionstamp, 2);
		System.out.println("vt2: " + vt2 + " ; has incomplete: " + vt2.hasIncompleteVersionstamp());
		Tuple vt3 = Tuple.from("complete nested: ", vt1, 3);
		System.out.println("vt3: " + vt3 + " ; has incomplete: " + vt3.hasIncompleteVersionstamp());
		Tuple vt4 = Tuple.from("incomplete nested: ", vt2, 4);
		System.out.println("vt4: " + vt4 + " ; has incomplete: " + vt4.hasIncompleteVersionstamp());
		Tuple vt5 = Tuple.from("complete with null: ", null, completeVersionstamp, 5);
		System.out.println("vt5: " + vt5 + " ; has incomplete: " + vt5.hasIncompleteVersionstamp());
		Tuple vt6 = Tuple.from("complete with null: ", null, incompleteVersionstamp, 6);
		System.out.println("vt6: " + vt6 + " ; has incomplete: " + vt6.hasIncompleteVersionstamp());
	}

	private static Tuple createTuple(int items) {
		List<Object> elements = new ArrayList<>(items);
		for(int i = 0; i < items; i++) {
			elements.add(new byte[]{99});
		}
		long start = System.currentTimeMillis();
		Tuple t = Tuple.fromList(elements);
		t.pack();
		System.out.println("Took " + (System.currentTimeMillis() - start) + " ms for " + items + " (" + elements.size() + ")");
		return t;
	}
}
