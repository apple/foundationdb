/*
 * table.go
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

type Table struct {
	row, col subspace.Subspace
}

func (tbl *Table) NewTable(ss subspace.Subspace) {
	tbl.row = ss.Sub("row")
	tbl.col = ss.Sub("col")
}

func (tbl Table) TableSetCell(trtr fdb.Transactor, row, column int, value interface{}) {
	trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		tr.Set(tbl.row.Pack(tuple.Tuple{row, column}), _pack(value))
		tr.Set(tbl.col.Pack(tuple.Tuple{column, row}), _pack(value))
		return nil, nil
	})
}

func (tbl Table) TableGetCell(trtr fdb.Transactor, row, column int) interface{} {
	item, _ := trtr.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		i := rtr.Get(tbl.row.Pack(tuple.Tuple{row, column})).MustGet()
		return i, nil
	})
	return _unpack(item.([]byte))[0]
}

func (tbl Table) TableSetRow(trtr fdb.Transactor, row int, cols ...interface{}) {
	trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		kr, err := fdb.PrefixRange(tbl.row.Pack(tuple.Tuple{row}))
		if err != nil {
			return nil, err
		}

		tr.ClearRange(kr)

		for c, v := range cols {
			tbl.TableSetCell(tr, row, c, v)
		}
		return nil, nil
	})
	return
}

func (tbl Table) TableGetRow(tr fdb.ReadTransactor, row int) ([]interface{}, error) {
	item, err := tr.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		kr, e := fdb.PrefixRange(tbl.row.Pack(tuple.Tuple{row}))
		if e != nil {
			return nil, e
		}

		slice, e := rtr.GetRange(kr, fdb.RangeOptions{0, -1, false}).GetSliceWithError()
		if e != nil {
			return nil, e
		}

		ret := make([]interface{}, len(slice))

		for i, v := range slice {
			ret[i] = _unpack(v.Value)[0]
		}

		return ret, nil
	})
	if err != nil {
		return nil, err
	}
	return item.([]interface{}), nil
}

func (tbl Table) TableGetCol(tr fdb.ReadTransactor, col int) ([]interface{}, error) {
	item, err := tr.ReadTransact(func(rtr fdb.ReadTransaction) (interface{}, error) {
		kr, e := fdb.PrefixRange(tbl.col.Pack(tuple.Tuple{col}))
		if e != nil {
			return nil, e
		}

		slice, e := rtr.GetRange(kr, fdb.RangeOptions{0, -1, false}).GetSliceWithError()
		if e != nil {
			return nil, e
		}

		ret := make([]interface{}, len(slice))

		for i, v := range slice {
			ret[i] = _unpack(v.Value)[0]
		}

		return ret, nil
	})
	if err != nil {
		return nil, err
	}
	return item.([]interface{}), nil
}

func main() {
	fdb.MustAPIVersion(720)

	db := fdb.MustOpenDefault()

	TableDemoDir, err := directory.CreateOrOpen(db, []string{"Graph"}, nil)
	if err != nil {
		log.Fatal(err)
	}

	clear_subspace(db, TableDemoDir)

	var g Table

	g.NewTable(TableDemoDir)

	g.TableSetCell(db, 0, 0, "Hello")
	g.TableSetCell(db, 0, 1, "World")

	fmt.Println(g.TableGetCell(db, 0, 0).(string))
	fmt.Println(g.TableGetCell(db, 0, 1).(string))

	g.TableSetRow(db, 1, "Hello", "World", "Again!", 1)
	fmt.Println(g.TableGetRow(db, 1))
}
