/*
 * AbstractWorkload.java
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

package com.apple.foundationdb.testing;

import com.apple.foundationdb.Database;

import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.SynchronousQueue;

import java.util.Map;

public abstract class AbstractWorkload {
	protected WorkloadContext context;
	private ThreadPoolExecutor executorService;

	public AbstractWorkload(WorkloadContext context) {
		this.context = context;
		long contextID = context.getProcessID();
		executorService =
			new ThreadPoolExecutor(1, 2,
								   10, TimeUnit.SECONDS,
								   new SynchronousQueue<>()) {
				@Override
				protected void beforeExecute(Thread t, Runnable r) {
					context.setProcessID(contextID);
					super.beforeExecute(t, r);
				}
			};
	}

	protected Executor getExecutor() {
		return executorService;
	}

	protected abstract void setup(Database db, Promise promise);
	protected abstract void start(Database db, Promise promise);
	protected abstract void check(Database db, Promise promise);
	protected List<PerfMetric> getMetrics() {
		return new ArrayList<PerfMetric>();
	}
	protected double getCheckTimeout() {
		return 3000;
	}

	private void shutdown() {
		executorService.shutdown();
	}

	private static long logger;
	public static void log(int severity, String message, Map<String, String> details) {
		log(logger, severity, message, details);
	}
	private static native void log(long logger, int severity, String message, Map<String, String> details);
}
