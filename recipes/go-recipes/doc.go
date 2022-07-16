/*
 * doc.go
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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"

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

const EmptyObject int = -1
const EmptyList int = -2

func ToTuples(item interface{}) []tuple.Tuple {
	switch i := item.(type) {
	case []interface{}:
		if len(i) == 0 {
			return []tuple.Tuple{{EmptyList}}
		}
		tuples := make([]tuple.Tuple, 0)
		for i, v := range i {
			for _, t := range ToTuples(v) {
				tuples = append(tuples, append(tuple.Tuple{i}, t...))
			}
		}
		return tuples
	case map[string]interface{}:
		if len(i) == 0 {
			return []tuple.Tuple{{EmptyObject}}
		}
		tuples := make([]tuple.Tuple, 0)
		for k, v := range i {
			for _, t := range ToTuples(v) {
				tuples = append(tuples, append(tuple.Tuple{k}, t...))
			}
		}
		return tuples
	default:
		return []tuple.Tuple{{i}}
	}
	return nil
}

func FromTuples(tuples []tuple.Tuple) interface{} {
	//fmt.Println(tuples)
	if len(tuples) == 0 {
		return nil
	}
	first := tuples[0]
	if len(first) == 1 {
		return first[0]
	}
	if first[0] == EmptyObject {
		return make(map[string]interface{}, 0)
	}
	if first[0] == EmptyList {
		return make([]interface{}, 0)
	}

	group := make(map[string][]tuple.Tuple)

	for _, t := range tuples {
		k := string(_pack(t[0]))
		_, ok := group[k]
		if !ok {
			group[k] = make([]tuple.Tuple, 0)
		}
		group[k] = append(group[k], t[0:])
	}

	switch first[0].(type) {
	case int64:
		res := make([]interface{}, 0)
		for _, g := range group {
			subtup := make([]tuple.Tuple, 0)
			for _, t := range g {
				subtup = append(subtup, t[1:])
			}
			res = append(res, FromTuples(subtup))
		}
		return res
	default:
		res := make(map[string]interface{})
		for _, g := range group {
			subtup := make([]tuple.Tuple, 0)
			for _, t := range g {
				subtup = append(subtup, t[1:])
			}
			res[g[0][0].(string)] = FromTuples(subtup)
		}
		return res
	}
}

type Doc struct {
	DocSS subspace.Subspace
}

func (doc Doc) InsertDoc(trtr fdb.Transactor, docdata []byte) int {
	var data interface{}
	json.Unmarshal(docdata, &data)
	docid := 0
	switch d := data.(type) {
	case map[string]interface{}:
		temp, ok := d["doc_id"]
		if !ok {
			docid = doc._GetNewID(trtr)
			d["doc_id"] = docid
		} else {
			docid = temp.(int)
		}
		tuples := ToTuples(d)
		trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
			for _, t := range tuples {
				tr.Set(doc.DocSS.Pack(append(tuple.Tuple{d["doc_id"]}, t[0:len(t)-1]...)), _pack(t[len(t)-1]))
			}
			return nil, nil
		})
	}
	return docid
}

func (doc Doc) _GetNewID(trtr fdb.Transactor) int {
	new_id := rand.Intn(100000007)
	trtr.Transact(func(tr fdb.Transaction) (interface{}, error) {
		for true {
			new_id = rand.Intn(100000007)
			rp, err := fdb.PrefixRange(doc.DocSS.Pack(tuple.Tuple{new_id}))
			if err != nil {
				continue
			}

			res, err := tr.GetRange(rp, fdb.RangeOptions{1, -1, false}).GetSliceWithError()
			if len(res) == 0 {
				break
			}
		}
		return nil, nil
	})
	return new_id
}

func (doc Doc) GetDoc(trtr fdb.Transactor, doc_id int) interface{} {
	tuples := make([]tuple.Tuple, 0)
	trtr.ReadTransact(func(tr fdb.ReadTransaction) (interface{}, error) {
		kr, err := fdb.PrefixRange(doc.DocSS.Pack(tuple.Tuple{doc_id}))
		if err != nil {
			panic(err)
		}

		items := tr.GetRange(kr, fdb.RangeOptions{}).Iterator()

		for items.Advance() {
			v := items.MustGet()
			tup, err := doc.DocSS.Unpack(v.Key)
			if err != nil {
				panic(err)
			}
			tuples = append(tuples, append(tup[1:], _unpack(v.Value)...))
		}
		return nil, nil
	})

	return FromTuples(tuples)
}

func main() {
	fdb.MustAPIVersion(720)

	db := fdb.MustOpenDefault()

	DocDemoDir, err := directory.CreateOrOpen(db, []string{"docdemo"}, nil)
	if err != nil {
		panic(err)
	}

	clear_subspace(db, DocDemoDir)

	mydoc := Doc{DocDemoDir}

	docdata, err := ioutil.ReadFile("./doctestjson.json")

	id := mydoc.InsertDoc(db, docdata)
	fmt.Println(mydoc.GetDoc(db, id), id)

}
