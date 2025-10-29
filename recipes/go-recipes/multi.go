/*
 * multi.go
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
	//"log"
	//"time"
	"github.com/apple/foundationdb/bindings/go/src/fdb"
	"github.com/apple/foundationdb/bindings/go/src/fdb/directory"
	"github.com/apple/foundationdb/bindings/go/src/fdb/subspace"
	"github.com/apple/foundationdb/bindings/go/src/fdb/tuple"
)

const API_VERSION int = 800

func clear_subspace(db fdb.Transactor, ss subspace.Subspace) {
	db.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.ClearRange(ss)
		return nil, nil
	})
	return

}

type MultiMap struct {
	MapSS    subspace.Subspace
	Pos, Neg []byte
}

func (multi *MultiMap) NewMultiMap(ss subspace.Subspace) {
	multi.MapSS = ss
	multi.Pos, multi.Neg = []byte{1, 0, 0, 0}, []byte{255, 255, 255, 255}
}

func (multi MultiMap) MultiAdd(trtr fdb.Transactor, index, value interface{}) {
	trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Add(multi.MapSS.Pack(tuple.Tuple{index, value}), multi.Pos)
		return nil, nil
	})
}

func (multi MultiMap) MultiSubtract(trtr fdb.Transactor, index, value interface{}) {
	trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		item := tr.Get(multi.MapSS.Pack(tuple.Tuple{index, value})).MustGet()

		if item == nil {
			tr.Clear(multi.MapSS.Pack(tuple.Tuple{index, value}))
		} else if item[0] > 1 || item[1] > 0 || item[2] > 0 || item[3] > 0 {
			tr.Add(multi.MapSS.Pack(tuple.Tuple{index, value}), multi.Neg)
		}
		return item, nil
	})
}

func (multi MultiMap) MultiGet(tr fdb.ReadTransactor, index int) (ret []interface{}, err error) {
	_, err = tr.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		pr, err := fdb.PrefixRange(multi.MapSS.Pack(tuple.Tuple{index}))
		if err != nil {
			return nil, err
		}
		kvs := tr.GetRange(pr, fdb.RangeOptions{0, -1, false}).GetSliceOrPanic()
		ret := make([]interface{}, len(kvs))
		i := 0
		for _, kv := range kvs {
			temp, err := multi.MapSS.Unpack(kv.Key)
			if err != nil {
				return nil, err
			}
			ret[i] = temp[1]
			i++
		}
		return nil, nil
	})
	return
}

func (multi MultiMap) MultiGetCounts(trtr fdb.Transactor, index interface{}) (map[interface{}]int, error) {
	i, err := trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kr, err := fdb.PrefixRange(multi.MapSS.Pack(tuple.Tuple{}))
		if err != nil {
			return nil, err
		}

		ks, err := tr.GetRange(kr, fdb.RangeOptions{}).GetSliceWithError()

		counts := make(map[interface{}]int)

		for _, v := range ks {
			bs := v.Value
			k, err := multi.MapSS.Unpack(v.Key)
			if err != nil {
				return nil, err
			}
			counts[k[1]] = 0
			for i, j := 0, 1; i < len(bs); i++ {
				counts[k[1]] += int(bs[i]) * j
				j = j << 4
			}
		}
		return counts, nil
	})
	return i.(map[interface{}]int), err
}

func (multi MultiMap) MultiIsElement(trtr fdb.Transactor, index, value interface{}) bool {
	item, _ := trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		r := tr.Get(multi.MapSS.Pack(tuple.Tuple{index, value})).MustGet()
		if r == nil {
			return false, nil
		}
		return true, nil
	})
	return item.(bool)
}

func main() {
	fdb.MustAPIVersion(API_VERSION)

	db := fdb.MustOpenDefault()

	MultiMapDemoDir, _ := directory.CreateOrOpen(db, []string{"Multi"}, nil)

	clear_subspace(db, MultiMapDemoDir)

	var m MultiMap

	m.NewMultiMap(MultiMapDemoDir)

	m.MultiAdd(db, 1, 2)
	m.MultiAdd(db, 1, 2)
	m.MultiSubtract(db, 1, 2)
	m.MultiAdd(db, 1, 4)

	fmt.Println(m.MultiIsElement(db, 1, 2))
	fmt.Println(m.MultiGetCounts(db, 1))
}
