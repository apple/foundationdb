/*
 * EventKeeper.java
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

import java.util.concurrent.TimeUnit;

/**
 * A device for externally instrumenting the FDB java driver, for monitoring
 * purposes.
 *
 * Note that implementations as expected to be thread-safe, and may be manipulated
 * from multiple threads even in nicely single-threaded-looking applications.
 */
public interface EventKeeper {

	/**
	 * Count the number of events which occurred.
	 *
	 * @param event the event which occurred
	 * @param amt the number of times that even occurred
	 */
	void count(Event event, long amt);

	/**
	 * Convenience method to add 1 to the number of events which occurred.
	 *
	 * @param event the event which occurred.
	 */
	default void increment(Event event) { count(event, 1L); }

	/**
	 * Count the time taken to perform an event, in nanoseconds.
	 *
	 * Note that {@code event.isTimeEvent()} should return true here.
	 *
	 * @param event the event which was timed (the event should be a time event).
	 * @param nanos the amount of time taken (in nanoseconds)
	 */
	void timeNanos(Event event, long nanos);

	/**
	 * Count the time taken to perform an action, in the specified units.
	 *
	 * Note that {@code event.isTimeEvent()} should return true.
	 *
	 * @param event the event which was timed.
	 * @param duration the time taken
	 * @param theUnit the unit of time in which the time measurement was taken
	 */
	default void time(Event event, long duration, TimeUnit theUnit) { timeNanos(event, theUnit.toNanos(duration)); }

	/**
	 * Get the number of events which occurred since this timer was created.
	 *
	 * If the event was never recorded, then this returns 0.
	 *
	 * @param event the event to get the count for
	 * @return the number of times the event was triggered. If the event has never been triggered,
	 * then this returns 0
	 */
	long getCount(Event event);

	/**
	 * Get the amount of time taken by this event, in nanoseconds.
	 *
	 * @param event the event to get the time for
	 * @return  the total time measured for this event, in nanoseconds. If the event was never recorded,
	 * return 0 instead.
	 */
	long getTimeNanos(Event event);

	/**
	 * Get the amount of time taken by this event, in the specified units.
	 *
	 * Important note: If the time that was measured in nanoseconds does not evenly divide the unit that
	 * is specified (which is likely, considering time), then some precision may be lost in the conversion. Use
	 * this carefully.
	 *
	 * @param event the event to get the time for
	 * @param theUnit the unit to get time in
	 * @return  the total time measured for this event, in the specified unit. If the event was never recorded,
	 * return 0.
	 */
	default long getTime(Event event, TimeUnit theUnit) {
		return theUnit.convert(getTimeNanos(event), TimeUnit.NANOSECONDS);
	}

	/**
	 * Marker interface for tracking the specific type of event that occurs, and metadata about said event.
	 *
	 * Implementations should be sure to provide a quality {@code equals} and {@code hashCode}.
	 */
	interface Event {
		/**
		 * @return the name of this event, as a unique string. This name should generally be unique, because
		 * it's likely that {@code EventKeeper} implementations will rely on this for uniqueness.
		 */
		String name();

		/**
		 * @return true if this event represents a timed event, rather than a counter event.
		 */
		default boolean isTimeEvent() { return false; };
	}

	/**
	 * An enumeration of static events which occur within the FDB Java driver.
	 */
	enum Events implements Event {
		/**
		 * The number of JNI calls that were exercised.
		 */
		JNI_CALL,

		/**
		 * The total number of bytes pulled from the native layer, including length delimiters., from
		 * {@link Transaction#get(byte[])}, {@link Transaction#getKey(KeySelector)},
		 * {@link Transaction#getRange(KeySelector, KeySelector)} (and related method
		 * overrides), or any other read-type operation which occurs on a Transaction
		 */
		BYTES_FETCHED,

		/**
		 * The number of times a DirectBuffer was used to transfer a range query chunk
		 * across the JNI boundary
		 */
		RANGE_QUERY_DIRECT_BUFFER_HIT,
		/**
		 * The number of times a range query chunk was unable to use a DirectBuffer to
		 * transfer data across the JNI boundary
		 */
		RANGE_QUERY_DIRECT_BUFFER_MISS,
		/**
		 * The number of direct fetches made during a range query
		 */
		RANGE_QUERY_FETCHES,
		/**
		 * The number of tuples fetched during a range query
		 */
		RANGE_QUERY_RECORDS_FETCHED,
		/**
		 * The number of times a range query chunk fetch failed
		 */
		RANGE_QUERY_CHUNK_FAILED,
		/**
		 * The time taken to perform an internal `getRange` fetch, in nanoseconds
		 */
		RANGE_QUERY_FETCH_TIME_NANOS {
			@Override
			public boolean isTimeEvent() {
				return true;
			}
		};
	}
}
