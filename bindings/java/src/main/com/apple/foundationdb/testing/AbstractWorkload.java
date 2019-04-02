/*
 * AbstractWorkload.java
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2019 Apple Inc. and the FoundationDB project authors
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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.SynchronousQueue;

import java.util.Map;

public abstract class AbstractWorkload {
	private static final Class<?>[] parameters = new Class<?>[]{URL.class};
	protected WorkloadContext context;
	private ThreadPoolExecutor executorService;

	public AbstractWorkload(WorkloadContext context) {
		this.context = context;
		executorService =
        new ThreadPoolExecutor(1, 2,
            10, TimeUnit.SECONDS,
			new SynchronousQueue<>()) {
			@Override
			protected void beforeExecute(Thread t, Runnable r) {
				setProcessID(context.getProcessID());
				super.beforeExecute(t, r);
			}
		};
	}

	private Executor getExecutor() {
		return executorService;
	}

	public abstract void setup(Database db);
	public abstract void start(Database db);
	public abstract boolean check(Database db);
	public double getCheckTimeout() {
		return 3000;
	}

	private void setup(Database db, long voidCallback) {
		AbstractWorkload self = this;
		getExecutor().execute(new Runnable(){
			public void run() {
				self.setup(db);
				self.sendVoid(voidCallback);
			}
		});
	}
	private void start(Database db, long voidCallback) {
		AbstractWorkload self = this;
		getExecutor().execute(new Runnable(){
			public void run() {
				self.start(db);
				self.sendVoid(voidCallback);
			}
		});
	}
	private void check(Database db, long boolCallback) {
		AbstractWorkload self = this;
		getExecutor().execute(new Runnable(){
			public void run() {
				boolean res = self.check(db);
				self.sendBool(boolCallback, res);
			}
		});
	}

	private void shutdown() {
		executorService.shutdown();
	}

	public native void log(int severity, String message, Map<String, String> details);
	private native void setProcessID(long processID);
	private native void sendVoid(long handle);
	private native void sendBool(long handle, boolean value);

	// Helper functions to add to the class path at Runtime - will be called
	// from C++
	private static void addFile(String s) throws IOException {
		File f = new File(s);
		addFile(f);
	}

	private static void addFile(File f) throws IOException {
		addURL(f.toURI().toURL());
	}

	private static void addURL(URL u) throws IOException {
		URLClassLoader sysLoader = (URLClassLoader) ClassLoader.getSystemClassLoader();
		Class<URLClassLoader> sysClass = URLClassLoader.class;

		try {
			Method method = sysClass.getDeclaredMethod("addURL", parameters);
			method.setAccessible(true);
			method.invoke(sysLoader, new Object[]{u});
		} catch (Throwable t) {
			t.printStackTrace();
			throw new IOException("Error, could not add URL to system classloader");
		}
	}
}
