/*
 * priority.go
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
	"log"
	"math/rand"

	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const API_VERSION int = 800

func clear_subspace(trtr fdb.Transactor, sub subspace.Subspace) error {
	_, err := trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(sub)
		return nil, nil
	})
	return err
}

func _pack(t interface{}) []byte {
	return tuple.Tuple{t}.Pack()
}

func _unpack(t []byte) tuple.Tuple {
	i, err := tuple.Unpack(t)
	if err != nil {
		return nil
	}
	return i
}

type Priority struct {
	PrioritySS subspace.Subspace
}

func (prty Priority) Push(trtr fdb.Transactor, value interface{}, priority int) {
	trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(prty.PrioritySS.Pack(tuple.Tuple{priority, prty._NextCount(tr, priority), rand.Intn(20)}), _pack(value))
		return nil, nil
	})
}

func (prty Priority) _NextCount(trtr fdb.Transactor, priority int) int {
	res, err := trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kr, err := fdb.PrefixRange(prty.PrioritySS.Pack(tuple.Tuple{priority}))
		if err != nil {
			return nil, err
		}

		ks, err := tr.Snapshot().GetRange(kr, fdb.RangeOptions{1, -1, true}).GetSliceWithError()
		if err != nil {
			return nil, err
		}

		if len(ks) == 0 {
			return 0, nil
		}
		k, err := prty.PrioritySS.Unpack(ks[0].Key)
		if err != nil {
			return nil, err
		}
		return k[0].(int) + 1, nil
	})
	if err != nil {
		return 0
	}
	return res.(int)
}

func (prty Priority) Pop(trtr fdb.Transactor, max bool) interface{} {
	res, _ := trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		ks, err := tr.GetRange(prty.PrioritySS, fdb.RangeOptions{1, -1, max}).GetSliceWithError()
		if err != nil {
			return nil, err
		}

		if len(ks) == 0 {
			return nil, nil
		}
		tr.Clear(ks[0].Key)
		return _unpack(ks[0].Value)[0], nil
	})
	return res
}

func (prty Priority) Peek(trtr fdb.Transactor, max bool) interface{} {
	res, _ := trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		ks, err := tr.GetRange(prty.PrioritySS, fdb.RangeOptions{1, -1, max}).GetSliceWithError()
		if err != nil {
			return nil, err
		}
		if len(ks) == 0 {
			return nil, nil
		}

		return _unpack(ks[0].Value)[0], nil
	})
	return res
}

func main() {
	fdb.MustAPIVersion(API_VERSION)

	db := fdb.MustOpenDefault()

	PriorityDemoDir, err := directory.CreateOrOpen(db, []string{"Priority"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	clear_subspace(db, PriorityDemoDir)

	var p Priority

	p = Priority{PriorityDemoDir}

	p.Push(db, "Hello World!", 0)
	fmt.Println(p.Pop(db, false))

	p.Push(db, "a", 1)
	p.Push(db, "b", 5)
	p.Push(db, "c", 2)
	p.Push(db, "d", 4)
	p.Push(db, "e", 3)

	fmt.Println(p.Peek(db, false))
	fmt.Println(p.Peek(db, false))

	fmt.Println(p.Pop(db, false))
	fmt.Println(p.Pop(db, false))
	fmt.Println(p.Pop(db, false))
	fmt.Println(p.Pop(db, false))
	fmt.Println(p.Pop(db, false))
}
