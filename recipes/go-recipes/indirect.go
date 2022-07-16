/*
 * indirect.go
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

package main

import (
	"fmt"
	"log"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

func clear_subspace(trtr fdb.Transactor, sub subspace.Subspace) error {
	_, err := trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(sub)
		return nil, nil
	})
	return err
}

func print_subspace(trtr fdb.Transactor, sub subspace.Subspace) {
	trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		k := tr.GetRange(sub, fdb.RangeOptions{0, -1, false}).Iterator()

		for k.Advance() {
			fmt.Println(_unpack(k.MustGet().Value))
		}
		return nil, nil
	})
}

func _pack(t interface{}) []byte {
	return tuple.Tuple{t}.Pack()
}

func _unpack(t []byte) tuple.Tuple {
	i, e := tuple.Unpack(t)
	if e != nil {
		return nil
	}
	return i
}

type Workspace struct {
	Dir directory.Directory
	db  fdb.Database
}

func (wrkspc Workspace) _Update(trtr fdb.Transactor) {
	trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		_, err := wrkspc.Dir.Remove(tr, []string{"current"})
		if err != nil {
			log.Fatal(err)
		}

		_, err = wrkspc.Dir.Move(tr, []string{"new"}, []string{"current"})
		return nil, err
	})
}

func (wrkspc Workspace) GetCurrent() (dir directory.DirectorySubspace, err error) {
	dir, err = wrkspc.Dir.CreateOrOpen(wrkspc.db, []string{"current"}, nil)
	return
}

func (wrkspc Workspace) Session(foo func(directory.DirectorySubspace)) (err error) {
	newdir, err := wrkspc.Dir.CreateOrOpen(wrkspc.db, []string{"new"}, nil)
	if err != nil {
		return
	}
	foo(newdir)
	wrkspc._Update(wrkspc.db)
	return
}

func main() {
	fdb.MustAPIVersion(720)

	db := fdb.MustOpenDefault()

	WorkspaceDemoDir, err := directory.CreateOrOpen(db, []string{"Workspace"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	clear_subspace(db, WorkspaceDemoDir)

	w := Workspace{WorkspaceDemoDir, db}
	current, err := w.GetCurrent()

	clear_subspace(db, current)

	db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(current.Pack(tuple.Tuple{"a"}), _pack("Hello"))
		return nil, nil
	})

	print_subspace(db, current)

	w.Session(func(dir directory.DirectorySubspace) {
		db.Transact(func(tr fdb.Transaction) (interface{}, error) {
			tr.Set(dir.Pack(tuple.Tuple{"b"}), _pack("World"))
			return nil, nil
		})
	})
	current, err = w.GetCurrent()
	if err != nil {
		log.Fatal(err)
	}
	print_subspace(db, current)
}
