/*
 * MapEventKeeper.java
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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A simple map-based EventKeeper.
 *
 * This class is thread-safe(per the {@link EventKeeper} spec). It holds all counters in memory;
 */
public class MapEventKeeper implements EventKeeper {
	private final ConcurrentMap<Event, AtomicLong> map = new ConcurrentHashMap<>();

	@Override
	public void count(Event event, long amt) {
		AtomicLong cnt = map.computeIfAbsent(event, (l) -> new AtomicLong(0L));
		cnt.addAndGet(amt);
	}

	@Override
	public void timeNanos(Event event, long nanos) {
		count(event, nanos);
	}

	@Override
	public long getCount(Event event) {
		AtomicLong lng = map.get(event);
		if (lng == null) {
			return 0L;
		}
		return lng.get();
	}

	@Override
	public long getTimeNanos(Event event) {
		return getCount(event);
	}
}
