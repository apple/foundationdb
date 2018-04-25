/*
 * TesterArgs.java
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

import java.util.ArrayList;
import java.util.List;

import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;

public class TesterArgs {
	private String outputDirectory;
	private boolean multiversionApi;
	private boolean callbacksOnExternalThread;
	private boolean externalClient;
	private Subspace subspace;
	private List<String> testsToRun;

	private TesterArgs(String outputDirectory, boolean multiversionApi, boolean callbacksOnExternalThread, boolean externalClient, Subspace subspace, List<String> testsToRun) {
		this.outputDirectory = outputDirectory;
		this.multiversionApi = multiversionApi;
		this.callbacksOnExternalThread = callbacksOnExternalThread;
		this.externalClient = externalClient;
		this.subspace = subspace;
		this.testsToRun = testsToRun;
	}

	public static void printUsage() {
		String usage = "Arguments:  [-o/--output-directory DIR] [--disable-multiversion-api] [--enable-callbacks-on-external-threads] [--use-external-client] [--tests-to-run TEST [TEST ...]] [-h/--help]\n" +
					   "\n" +
					   "Arguments:\n" +
					   "   -o/--output-directory DIR                Directory to store JSON output. If not set, the current directory is used.\n" +
					   "   --disable-multiversion-api               Disables the multi-version client API\n" +
					   "   --enable-callbacks-on-external-threads   Allows callbacks to be called on threads created by the client library.\n" +
					   "   --use-external-client                    Connect to the server using an external client.\n" +
					   "   --tests-to-run TEST [TEST ...]           List of test names to run.\n" +
					   "   -h/--help                                Print this help message and then quit.\n";

		System.out.print(usage);
	}

	/**
	 * Parses the argument strings into a <code>TesterArgs</code> instance.
	 * This will return <code>null</code> if the args include an argument telling
	 * it to print the help message and it will throw an {@link IllegalArgumentException}
	 * if it can't parse the arguments.
	 *
	 * @param args command-line args
	 * @return built instance or <code>null</code>
	 * @throws IllegalArgumentException if the arguments can't be parsed
	 */
	public static TesterArgs parseArgs(String[] args) {
		String outputDirectory = "";
		boolean multiversionApi = true;
		boolean callbacksOnExternalThread = false;
		boolean externalClient = false;
		Subspace subspace = new Subspace();
		List<String> testsToRun = new ArrayList<String>();

		for (int i = 0; i < args.length; i++) {
			String arg = args[i];
			if (arg.equals("-o") || arg.equals("--output-directory")) {
				if (i + 1 < args.length) {
					outputDirectory = args[++i];
				} else {
					System.out.println("No output directory specified for argument " + arg + "\n");
					printUsage();
					throw new IllegalArgumentException("No output directory specified for argument " + arg);
				}
			} else if (arg.equals("--subspace")) {
				if (i + 1 < args.length) {
					subspace = new Subspace(Tuple.from(args[++i]));
				} else {
					System.out.println("No subspace specified for argument " + arg + "\n");
					printUsage();
					throw new IllegalArgumentException("Not subspace specified for argument " + arg);
				}
			} else if (arg.equals("--disable-multiversion-api")) {
				multiversionApi = false;
			} else if (arg.equals("--enable-callbacks-on-external-threads")) {
				callbacksOnExternalThread = true;
			} else if (arg.equals("--use-external-client")) {
				externalClient = true;
			} else if (arg.equals("--tests-to-run")) {
				if (i + 1 < args.length && args[i + 1].charAt(0) != '-') {
					int j;
					for (j = i + 1; j < args.length && args[j].charAt(0) != '-'; j++) {
						testsToRun.add(args[j]);
					}
					i = j - 1;
				} else {
					System.out.println("No tests specified with argument " + arg + "\n");
					printUsage();
					throw new IllegalArgumentException("No tests specified with argument " + arg);
				}
			} else if (arg.equals("-h") || arg.equals("--help")) {
				printUsage();
				return null;
			} else {
				System.out.println("Unknown argument " + arg + "\n");
				printUsage();
				throw new IllegalArgumentException("Unknown argument " + arg);
			}
		}

		return new TesterArgs(outputDirectory, multiversionApi, callbacksOnExternalThread, externalClient, subspace, testsToRun);
	}

	// Accessors.

	public String getOutputDirectory() {
		return outputDirectory;
	}

	public boolean useMultiversionApi() {
		return multiversionApi;
	}

	public boolean putCallbacksOnExternalThread() {
		return callbacksOnExternalThread;
	}

	public boolean useExternalClient() {
		return externalClient;
	}

	public Subspace getSubspace() {
		return subspace;
	}

	public List<String> getTestsToRun() {
		return testsToRun;
	}
}