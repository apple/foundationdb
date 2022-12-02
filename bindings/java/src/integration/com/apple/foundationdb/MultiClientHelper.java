/*
 * MultiClientHelper.java
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

import java.util.ArrayList;
import java.util.Collection;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/**
 * Callback to help define a multi-client scenario and ensure that 
 * the clients can be configured properly.
 */
public class MultiClientHelper implements BeforeAllCallback,AfterEachCallback{
	private String[] clusterFiles;
	private Collection<Database> openDatabases;
	
	public static String[] readClusterFromEnv() {
		/*
		 * Reads the cluster file lists from the ENV variable
		 * FDB_CLUSTERS.
		 */
		String clusterFilesProp = System.getenv("FDB_CLUSTER_FILE");
		if (clusterFilesProp == null) {
			throw new IllegalStateException("Missing FDB cluster connection file names");
		}

		return clusterFilesProp.split(";");
	}

	Collection<Database> openDatabases(FDB fdb){
		if(openDatabases!=null){
			return openDatabases;
		}
		if(clusterFiles==null){
			clusterFiles = readClusterFromEnv();
		}
		Collection<Database> dbs = new ArrayList<Database>();
		for (String arg : clusterFiles) {
			System.out.printf("Opening Cluster: %s\n", arg);
			dbs.add(fdb.open(arg));
		}

		this.openDatabases = dbs;
		return dbs;
	}

	@Override
	public void beforeAll(ExtensionContext arg0) throws Exception {
		clusterFiles = readClusterFromEnv();
	}

	@Override
	public void afterEach(ExtensionContext arg0) throws Exception {
		//close any databases that have been opened	
		if(openDatabases!=null){
			for(Database db : openDatabases){
				db.close();
			}
		}
		openDatabases = null;
	}

}
