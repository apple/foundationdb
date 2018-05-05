/*
 * package-info.java
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

/**
 * Provides tools for managing hierarchically related key subspaces.
 *  Directories are a recommended approach for
 *  administering applications. Each application should create or
 *  open at least one directory to manage its subspaces.<br>
 * <br>
 * Directories are identified by hierarchical paths analogous to the
 *  paths in a Unix-like file system. A path is represented as a tuple
 *  of strings. Each directory has an associated subspace used to store
 *  its content. The directory layer maps each path to a short prefix
 *  used for the corresponding subspace. In effect, directories provide
 *  a level of indirection for access to subspaces.<br>
 *  <br>
 *  See <a href="/foundationdb/developer-guide.html#developer-guide-directories">general
 *   directory documentation</a> for information about how directories work and
 *   interact with other parts of the built-in keyspace management features.
 */
package com.apple.foundationdb.directory;
