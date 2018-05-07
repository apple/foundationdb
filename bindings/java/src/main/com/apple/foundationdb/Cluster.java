/*
 * Cluster.java
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

package com.apple.foundationdb;

import java.nio.charset.Charset;
import java.util.concurrent.Executor;

/**
 * The {@code Cluster} represents a connection to a physical set of cooperating machines
 *  running FoundationDB. A {@code Cluster} is opened with a reference to a cluster file.<br>
 * <br>
 * <b>Note:</b> {@code Cluster} objects must be {@link #close closed} when no longer in use
 *  in order to free any associated resources.
 */
public class Cluster extends NativeObjectWrapper {
	private ClusterOptions options;
	private final Executor executor;

	private static final Charset UTF8 = Charset.forName("UTF-8");

	protected Cluster(long cPtr, Executor executor) {
		super(cPtr);
		this.executor = executor;
		this.options = new ClusterOptions((code, parameter) -> {
			pointerReadLock.lock();
			try {
				Cluster_setOption(getPtr(), code, parameter);
			} finally {
				pointerReadLock.unlock();
			}
		});
	}

	/**
	 * Returns a set of options that can be set on a {@code Cluster}. In the current version
	 * of the API, there are no options that can be set on a {@code Cluster}.
	 *
	 * @return a set of cluster-specific options affecting this {@code Cluster}
	 */
	public ClusterOptions options() {
		return options;
	}

	@Override
	protected void finalize() throws Throwable {
		try {
			checkUnclosed("Cluster");
			close();
		}
		finally {
			super.finalize();
		}
	}

	/**
	 * Creates a connection to a specific database on an <i>FDB</i> cluster.
	 *
	 * @return a {@code Future} that will be set to a {@code Database} upon
	 *         successful connection.
	 */
	public Database openDatabase() throws FDBException {
		return openDatabase(executor);
	}

	/**
	 * Creates a connection to a specific database on an <i>FDB</i> cluster.
	 *
	 * @param e the {@link Executor} to use when executing asynchronous callbacks for the database
	 *
	 * @return a {@code Future} that will be set to a {@code Database} upon
	 *         successful connection.
	 */
	public Database openDatabase(Executor e) throws FDBException {
		FutureDatabase futureDatabase;
		pointerReadLock.lock();
		try {
			futureDatabase = new FutureDatabase(Cluster_createDatabase(getPtr(), "DB".getBytes(UTF8)), e);
		} finally {
			pointerReadLock.unlock();
		}
		return futureDatabase.join();
	}

	@Override
	protected void closeInternal(long cPtr) {
		Cluster_dispose(cPtr);
	}

	private native void Cluster_dispose(long cPtr);
	private native long Cluster_createDatabase(long cPtr, byte[] dbName);
	private native void Cluster_setOption(long cPtr, int code, byte[] value) throws FDBException;
}
