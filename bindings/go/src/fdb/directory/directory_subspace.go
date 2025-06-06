/*
 * directory_subspace.go
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

// FoundationDB Go Directory Layer

package directory

import (
	"context"
	"fmt"
	"strings"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

// DirectorySubspace represents a Directory that may also be used as a Subspace
// to store key/value pairs. Subdirectories of a root directory (as returned by
// Root or NewDirectoryLayer) are DirectorySubspaces, and provide all methods of
// the Directory and subspace.Subspace interfaces.
type DirectorySubspace interface {
	subspace.Subspace
	Directory
}

type directorySubspace struct {
	subspace.Subspace
	dl    directoryLayer
	path  []string
	layer []byte
}

// String implements the fmt.Stringer interface and returns human-readable
// string representation of this object.
func (ds directorySubspace) String() string {
	var path string
	if len(ds.path) > 0 {
		path = "(" + strings.Join(ds.path, ",") + ")"
	} else {
		path = "nil"
	}
	return fmt.Sprintf("DirectorySubspace(%s, %s)", path, fdb.Printable(ds.Bytes()))
}

func (d directorySubspace) CreateOrOpen(ctx context.Context, t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return d.dl.CreateOrOpen(ctx, t, d.dl.partitionSubpath(d.path, path), layer)
}

func (d directorySubspace) Create(ctx context.Context, t fdb.Transactor, path []string, layer []byte) (DirectorySubspace, error) {
	return d.dl.Create(ctx, t, d.dl.partitionSubpath(d.path, path), layer)
}

func (d directorySubspace) CreatePrefix(ctx context.Context, t fdb.Transactor, path []string, layer []byte, prefix []byte) (DirectorySubspace, error) {
	return d.dl.CreatePrefix(ctx, t, d.dl.partitionSubpath(d.path, path), layer, prefix)
}

func (d directorySubspace) Open(ctx context.Context, rt fdb.ReadTransactor, path []string, layer []byte) (DirectorySubspace, error) {
	return d.dl.Open(ctx, rt, d.dl.partitionSubpath(d.path, path), layer)
}

func (d directorySubspace) MoveTo(ctx context.Context, t fdb.Transactor, newAbsolutePath []string) (DirectorySubspace, error) {
	return moveTo(ctx, t, d.dl, d.path, newAbsolutePath)
}

func (d directorySubspace) Move(ctx context.Context, t fdb.Transactor, oldPath []string, newPath []string) (DirectorySubspace, error) {
	return d.dl.Move(ctx, t, d.dl.partitionSubpath(d.path, oldPath), d.dl.partitionSubpath(d.path, newPath))
}

func (d directorySubspace) Remove(ctx context.Context, t fdb.Transactor, path []string) (bool, error) {
	return d.dl.Remove(ctx, t, d.dl.partitionSubpath(d.path, path))
}

func (d directorySubspace) Exists(ctx context.Context, rt fdb.ReadTransactor, path []string) (bool, error) {
	return d.dl.Exists(ctx, rt, d.dl.partitionSubpath(d.path, path))
}

func (d directorySubspace) List(ctx context.Context, rt fdb.ReadTransactor, path []string) ([]string, error) {
	return d.dl.List(ctx, rt, d.dl.partitionSubpath(d.path, path))
}

func (d directorySubspace) GetLayer() []byte {
	return d.layer
}

func (d directorySubspace) GetPath() []string {
	return d.path
}
