/*
 * node.go
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
	"bytes"
	"context"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
)

type node struct {
	subspace   subspace.Subspace
	path       []string
	targetPath []string
	_layer     fdb.FutureByteSlice
}

func (n *node) exists() bool {
	if n.subspace == nil {
		return false
	}
	return true
}

// prefetchMetadata will make sure that layer information starts being prefetched.
// If subspace is nil, this is a no-op and returns false.
// Caller is responsible for closing the future used to fetch layer information.
func (n *node) prefetchMetadata(rtr fdb.ReadTransaction) (*node, bool) {
	if n.exists() {
		n.layer(rtr)
		return n, true
	}
	return n, false
}

// layer will start a future to fetch layer information, unless one already exists.
// Caller is responsible for properly closing this future.
func (n *node) layer(rtr fdb.ReadTransaction) fdb.FutureByteSlice {
	if n._layer == nil {
		fv := rtr.Get(n.subspace.Sub([]byte("layer")))
		n._layer = fv
	}

	return n._layer
}

func (n *node) isInPartition(ctx context.Context, tr *fdb.Transaction, includeEmptySubpath bool) bool {
	return n.exists() && bytes.Compare(n._layer.MustGet(ctx), []byte("partition")) == 0 && (includeEmptySubpath || len(n.targetPath) > len(n.path))
}

func (n *node) getPartitionSubpath() []string {
	return n.targetPath[len(n.path):]
}

func (n *node) getContents(ctx context.Context, dl directoryLayer, tr *fdb.Transaction) (DirectorySubspace, error) {
	l, err := n._layer.Get(ctx)
	if err != nil {
		return nil, err
	}
	return dl.contentsOfNode(n.subspace, n.path, l)
}
