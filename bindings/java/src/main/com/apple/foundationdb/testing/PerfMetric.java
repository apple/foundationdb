/*
 * PerfMetric.java
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

public class PerfMetric {
	private String name;
	private double value;
	private boolean averaged;
	private String formatCode;

	public PerfMetric(String name, double value) {
		this(name, value, true, "0.3g");
	}

	public PerfMetric(String name, double value, boolean averaged) {
		this(name, value, averaged, "0.3g");
	}

	public PerfMetric(String name, double value, boolean averaged, String formatCode) {
		this.name = name;
		this.value = value;
		this.averaged = averaged;
		this.formatCode = formatCode;
	}

	public String getName() {
		return name;
	}
	public double getValue() {
		return value;
	}
	public boolean isAveraged() {
		return averaged;
	}
	public String getFormatCode() {
		return formatCode;
	}

	public void setName(String name) {
		this.name = name;
	}
	public void setValue(double value) {
		this.value = value;
	}
	public void setAveraged(boolean averaged) {
		this.averaged = averaged;
	}
	public void setFormatCode(String formatCode) {
		this.formatCode = formatCode;
	}
}
