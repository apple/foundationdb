/*
 * IterableComparator.java
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

import java.util.Comparator;
import java.util.Iterator;

/**
 * A {@link Tuple}-compatible {@link Comparator} that will sort {@link Iterable}s
 * in a manner that is consistent with the byte-representation of {@link Tuple}s.
 * In particular, if one has two {@link Tuple}s, {@code tuple1} and {@code tuple2},
 * it is the case that:
 *
 * <pre>
 * {@code
 *    tuple1.compareTo(tuple2)
 *      == new IterableComparator().compare(tuple1, tuple2)
 *      == new IterableComparator().compare(tuple1.getItems(), tuple2.getItems()),
 *      == ByteArrayUtil.compareUnsigned(tuple1.pack(), tuple2.pack())}
 * </pre>
 *
 * <p>
 * The individual elements of the {@link Iterable} must be of a type that can
 * be serialized by a {@link Tuple}. For items of identical types, they will be
 * sorted in a way that is consistent with their natural ordering with a few
 * caveats:
 * </p>
 *
 * <ul>
 *     <li>
 *         For floating point types, negative NaN values are sorted before all regular values, and
 *         positive NaN values are sorted after all regular values.
 *     </li>
 *     <li>
 *         Single-precision floating point numbers are sorted before
 *         all double-precision floating point numbers.
 *     </li>
 *     <li>
 *         UUIDs are sorted by their <i>unsigned</i> Big-Endian byte representation
 *         rather than their signed byte representation (which is the behavior of
 *         {@link java.util.UUID#compareTo(java.util.UUID) UUID.compareTo()}).
 *     </li>
 *     <li>Strings are sorted explicitly by their UTF-8 byte representation</li>
 *     <li>Nested {@link Tuple}s and {@link java.util.List List}s are sorted element-wise.</li>
 * </ul>
 */
public class IterableComparator implements Comparator<Iterable<?>> {
	/**
	 * Creates a new {@code IterableComparator}. This {@link Comparator} has
	 * no internal state.
	 */
	public IterableComparator() {}

	/**
	 * Compare two {@link Iterable}s in a way consistent with their
	 * byte representation. This is done element-wise and is consistent
	 * with a number of other ways of sorting {@link Tuple}s. This will
	 * raise an {@link IllegalArgumentException} if any of the items
	 * of either {@link Iterable} cannot be serialized by a {@link Tuple}.
	 *
	 * @param iterable1 the first {@link Iterable} of items
	 * @param iterable2 the second {@link Iterable} of items
	 * @return a negative number if the first iterable would sort before the second
	 * when serialized, a positive number if the opposite is true, and zero
	 * if the two are equal
	 */
	@Override
	public int compare(Iterable<?> iterable1, Iterable<?> iterable2) {
		Iterator<?> i1 = iterable1.iterator();
		Iterator<?> i2 = iterable2.iterator();

		while(i1.hasNext() && i2.hasNext()) {
			int itemComp = TupleUtil.compareItems(i1.next(), i2.next());
			if(itemComp != 0) {
				return itemComp;
			}
		}

		if(i1.hasNext()) {
			// iterable2 is a prefix of iterable1.
			return 1;
		}
		if(i2.hasNext()) {
			// iterable1 is a prefix of iterable2.
			return -1;
		}
		return 0;
	}
}
