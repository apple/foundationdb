/*
 * OSTest.java
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

package com.apple.foundationdb.test;

import java.io.InputStream;

public class OSTest {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		System.out.println("OS name: " + System.getProperty("os.name"));
		System.out.println("OS arch: " + System.getProperty("os.arch"));

		InputStream stream = OSTest.class.getResourceAsStream("/lib/linux/amd64/libfdb_java.so");
		System.out.println("Stream: " + stream);
	}

}
