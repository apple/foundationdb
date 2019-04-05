/*
 * IWorkload.java
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

import java.util.Map;

public class WorkloadContext {
	private Map<String, String> options;
	private int clientId, clientCount;
	long sharedRandomNumber, processID;

	public WorkloadContext(Map<String, String> options, int clientId, int clientCount, long sharedRandomNumber, long processID)
	{
		this.options = options;
		this.clientId = clientId;
		this.clientCount = clientCount;
		this.sharedRandomNumber = sharedRandomNumber;
		this.processID = processID;
	}

	public String getOption(String name, String defaultValue) {
		if (options.containsKey(name)) {
			return options.get(name);
		}
		return defaultValue;
	}

	public int getClientId() {
		return clientId;
	}

	public int getClientCount() {
		return clientCount;
	}

	public long getSharedRandomNumber() {
		return sharedRandomNumber;
	}

	public long getProcessID() {
		return processID;
	}
}
