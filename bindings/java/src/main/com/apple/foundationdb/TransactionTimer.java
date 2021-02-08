/*
 * TransactionTimer.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2021 Apple Inc. and the FoundationDB project authors
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
 * A thread-safe timer implementation which can be used to aggregate instrumented metrics in the underlying Java Driver.
 */
public interface TransactionTimer {

    void count(Event event, long amt);

    default void increment(Event event) {
        count(event,1L);
    }

    void timeNanos(Event event, long nanos);

    long getCount(Event event);

    long getTimeNanos(Event event);

    interface Event{
        String name();

        boolean isTimeEvent();
    }

    enum Events implements Event{
        /**
         * The number of JNI calls that were exercised.
         */
        JNI_CALL,

        /**
         * The total number of bytes fetched over the network, 
         * from {@link Transaction#get(byte[])}, {@link Transaction#getKey(KeySelector)}, {@link Transaction#getRange(KeySelector, KeySelector)} (and related method overrides), or any other
         * read-type operation which occurs on a Transaction
         */
        BYTES_FETCHED,

        /**
         * The number of times a DirectBuffer was used to transfer a range query chunk across the JNI boundary
         */
        RANGE_QUERY_DIRECT_BUFFER_HIT,
        /**
         * The number of times a range query chunk was unable to use a DirectBuffer to transfer data across the JNI boundary
         */
        RANGE_QUERY_DIRECT_BUFFER_MISS,
        /**
         * The number of direct fetches made during a range query
         */
        RANGE_QUERY_FETCHES,
        /**
         * The number of tuples fetched during a range query
         */
        RANGE_QUERY_TUPLES_FETCHED,
        /**
         * The number of times a range query chunk fetch failed 
         */
        RANGE_QUERY_CHUNK_FAILED, 
        RANGE_QUERY_FETCH_TIME_NANOS{
            @Override
            public boolean isTimeEvent(){
                return true;
            }
        };

        @Override
        public boolean isTimeEvent() {
            return false;
        }

    }
}
