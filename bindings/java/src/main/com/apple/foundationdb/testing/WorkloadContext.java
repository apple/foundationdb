/*
 * IWorkload.java
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

public class WorkloadContext {
	long impl;

	private WorkloadContext(long impl)
	{
		this.impl = impl;
	}

	public long getProcessID() {
		return getProcessID(impl);
	}

	public void setProcessID(long processID) {
		setProcessID(impl, processID);
	}

	public int getClientID() {
		return getClientID(impl);
	}

	public int getClientCount() {
		return getClientCount(impl);
	}

	public long getSharedRandomNumber() {
		return getSharedRandomNumber(impl);
	}

	public String getOption(String name, String defaultValue) {
		return getOption(impl, name, defaultValue);
	}
	public long getOption(String name, long defaultValue) {
		return getOption(impl, name, defaultValue);
	}
	public boolean getOption(String name, boolean defaultValue) {
		return getOption(impl, name, defaultValue);
	}
	public double getOption(String name, double defaultValue) {
		return getOption(impl, name, defaultValue);
	}

	private static native long getProcessID(long self);
	private static native void setProcessID(long self, long processID);
	private static native boolean getOption(long impl, String name, boolean defaultValue);
	private static native long getOption(long impl, String name, long defaultValue);
	private static native double getOption(long impl, String name, double defaultValue);
	private static native String getOption(long impl, String name, String defaultValue);
	private static native int getClientID(long self);
	private static native int getClientCount(long self);
	private static native long getSharedRandomNumber(long self);

}
