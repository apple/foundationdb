/*
 * blob.go
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

package main

import (
	"fmt"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const API_VERSION int = 800

const CHUNK_SIZE int = 5

func write_blob(t fdb.Transactor, blob_subspace subspace.Subspace, blob []byte) (err error) {

	_, err = t.Transact(func(tr fdb.Transaction) (interface{}, error) {

		if len(blob) == 0 {
			return nil, nil
		}

		for i := 0; i < len(blob); i += CHUNK_SIZE {
			if i+CHUNK_SIZE <= len(blob) {
				tr.Set(blob_subspace.Pack(tuple.Tuple{i}), blob[i:i+CHUNK_SIZE])
			} else {
				tr.Set(blob_subspace.Pack(tuple.Tuple{i}), blob[i:])
			}
		}
		return nil, nil
	})
	return
}

func read_blob(t fdb.ReadTransactor, blob_subspace subspace.Subspace) ([]byte, error) {

	blb, err := t.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {

		var blob []byte

		ri := rtr.GetRange(blob_subspace, fdb.RangeOptions{}).Iterator()

		for ri.Advance() {

			kv := ri.MustGet()

			blob = append(blob, rtr.Get(kv.Key).MustGet()...)

		}

		return blob, nil
	})

	if err != nil {

		return nil, err
	}

	return blb.([]byte), nil
}

func main() {
	fdb.MustAPIVersion(API_VERSION)

	db := fdb.MustOpenDefault()

	blobdir, err := directory.CreateOrOpen(db, []string{"blobdir"}, nil)

	if err != nil {
		fmt.Println(err)
	}

	blobspace := blobdir.Sub("blob")

	test := []byte("ABCDEFGHIJKLMNOPQRSTUVWXYZ")

	write_blob(db, blobspace, test)

	ret, err := read_blob(db, blobspace)

	if err == nil {
		fmt.Println(string(ret))
	}
}
