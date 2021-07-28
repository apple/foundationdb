/*
 * TestResult.java
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

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

public class TestResult {
	private long id;
	private Map<String,Map<String,Object>> kpis;
	private List<Throwable> errors;

	public TestResult(Random r) {
		id = Math.abs(r.nextLong());
		kpis = new TreeMap<String,Map<String,Object>>(); // Tree map because we will have to print this out.
		errors = new ArrayList<Throwable>();
	}

	public void addKpi(String name, Number value, String units) {
		TreeMap<String,Object> kpi = new TreeMap<String,Object>();
		kpi.put("value", value);
		kpi.put("units", units);
		kpis.put(name, kpi);
	}

	public void addError(Throwable t) {
		errors.add(t);
	}

	public void save(String directory) {
		String file = "javaresult-" + id + ".json";
		if(directory.length() > 0) {
			file = directory + "/" + file;
		}

		// TODO: Should we use a really JSON library?

		StringBuilder outputBuilder = new StringBuilder();
		outputBuilder.append('{');

		// Add KPIs:
		outputBuilder.append("\"kpis\": {");
		boolean firstKpi = true;
		for (Map.Entry<String,Map<String,Object>> kpi : kpis.entrySet()) {
			if (firstKpi) {
				firstKpi = false;
			} else {
				outputBuilder.append(", ");
			}

			outputBuilder.append("\"");
			outputBuilder.append(kpi.getKey());
			outputBuilder.append("\": {");

			boolean firstEntry = true;

			for (Map.Entry<String,Object> entry : kpi.getValue().entrySet()) {
				if (firstEntry) {
					firstEntry = false;
				} else {
					outputBuilder.append(", ");
				}

				outputBuilder.append("\"");
				outputBuilder.append(entry.getKey());
				outputBuilder.append("\": ");

				Object value = entry.getValue();
				if (value instanceof String) {
					outputBuilder.append("\"");
					outputBuilder.append((String)value);
					outputBuilder.append("\"");
				} else {
					outputBuilder.append(value.toString());
				}
			}

			outputBuilder.append("}");
		}
		outputBuilder.append("}, ");

		// Add errors:
		outputBuilder.append("\"errors\":[");
		boolean firstError = true;
		for (Throwable t : errors) {
			if (firstError) {
				firstError = false;
			} else {
				outputBuilder.append(", ");
			}

			StringBuilder msgBuilder = new StringBuilder();
			msgBuilder.append(t.getClass().toString());
			msgBuilder.append(": ");
			msgBuilder.append(t.getMessage()); // Escaping quotes. Yeah, this won't work in the general case....
			StackTraceElement[] stackTraceElements = t.getStackTrace();
			for (StackTraceElement element : stackTraceElements) {
				msgBuilder.append("\n    ");
				msgBuilder.append(element.toString());
			}
			outputBuilder.append('"');
			outputBuilder.append(msgBuilder.toString()
					.replace("\\", "\\\\")
					.replace("\"", "\\\"")
					.replace("\t", "\\t")
					.replace("\r", "\\r")
					.replace("\n", "\\n")
					.replace("\f", "\\f")
					.replace("\b", "\\b")
			);
			outputBuilder.append('"');
		}
		outputBuilder.append("]");

		outputBuilder.append('}');

		try (BufferedWriter writer = new BufferedWriter(new FileWriter(file))) {
			writer.write(outputBuilder.toString());
		} catch (IOException e) {
			System.out.println("Could not write results to file " + file);
			throw new RuntimeException("Could not save results: " + e.getMessage(), e);
		}
	}
}
