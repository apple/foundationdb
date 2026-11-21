/*
 * database_test.go
 *
 * This source file is part of the FoundationDB open source project
 *
 * Copyright 2013-2026 Apple Inc. and the FoundationDB project authors
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

// FoundationDB Go API

package fdb

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCloseZeroDatabase(t *testing.T) {
	var unopened Database
	unopened.Close()
}

func TestIndependentDatabaseOpens(t *testing.T) {
	MustAPIVersion(API_VERSION)
	clusterFile := filepath.Join(t.TempDir(), "fdb.cluster")
	if err := os.WriteFile(clusterFile, []byte("test:test@127.0.0.1:4500"), 0600); err != nil {
		t.Fatal(err)
	}

	first := MustOpenDatabase(clusterFile)
	second := MustOpenDatabase(clusterFile)
	if first.database != second.database {
		t.Fatal("overlapping opens did not share a cached database")
	}

	firstCopy := first
	first.Close()
	firstCopy.Close()
	if _, err := second.CreateTransaction(); err != nil {
		t.Fatalf("create transaction after closing the other handle: %v", err)
	}

	second.Close()
	reopened := MustOpenDatabase(clusterFile)
	defer reopened.Close()
	if reopened.database == first.database {
		t.Fatal("the last close did not evict the cached database")
	}
}
